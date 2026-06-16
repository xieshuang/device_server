package com.xsh.netty.codec;

import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 自定义协议解码器，继承 {@link ByteToMessageDecoder}。
 *
 * <p>支持 V1（11字节头部，无 sequenceId）和 V2（15字节头部，含 sequenceId）协议版本。
 * 根据 Version 字段自动选择解码逻辑。
 *
 * <p>V2 处理流程：
 * <ol>
 *   <li>先读取前7字节（Magic + Version + SerializationType + MsgType）</li>
 *   <li>根据 Version 确定头部长度：V1=11字节，V2=15字节</li>
 *   <li>校验魔数，非法则直接关闭连接</li>
 *   <li>V2 读取 sequenceId，V1 跳过</li>
 *   <li>读取 length，校验合法性（非负且不超过 maxFrameLength）</li>
 *   <li>检查剩余可读字节是否足够 Body 长度，不足则回滚读指针等待（粘包/半包处理）</li>
 *   <li>根据消息类型解码 Body</li>
 * </ol>
 *
 * <p>安全防护：
 * <ul>
 *   <li>非法魔数直接断开连接，防止恶意扫描</li>
 *   <li>length 上限校验（maxFrameLength），防止恶意客户端构造超大 Length 导致 OOM</li>
 * </ul>
 */
@Slf4j
public class CustomProtocolDecoder extends ByteToMessageDecoder {

    /** 单帧最大允许长度，超过此值将关闭连接，防止 OOM */
    private final int maxFrameLength;

    public CustomProtocolDecoder(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 先读取前7字节判断版本，至少需要 V1 的最小头部
        if (in.readableBytes() < MessageHeader.V1_BASE_LENGTH) {
            return;
        }

        // 标记读指针，用于半包回滚
        in.markReaderIndex();

        // 读取魔数
        int magicNumber = in.readInt();
        if (magicNumber != MessageHeader.MAGIC_NUMBER) {
            log.warn("非法魔数: 0x{}, 关闭连接: {}",
                    Integer.toHexString(magicNumber), ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        byte version = in.readByte();
        byte serializationType = in.readByte();
        byte msgType = in.readByte();

        // 根据 Version 确定头部长度和字段
        int sequenceId = 0;
        int headerLength;
        if (version >= 2) {
            // V2：需要读取 sequenceId
            if (in.readableBytes() < 4 + 4) { // sequenceId(4) + length(4) 至少需要8字节
                in.resetReaderIndex();
                return;
            }
            sequenceId = in.readInt();
            headerLength = MessageHeader.BASE_LENGTH; // 15
        } else {
            // V1：无 sequenceId
            if (in.readableBytes() < 4) { // length(4)
                in.resetReaderIndex();
                return;
            }
            headerLength = MessageHeader.V1_BASE_LENGTH; // 11
        }

        int length = in.readInt();

        // length 为负数，非法数据，断开连接
        if (length < 0) {
            log.warn("负数长度: {}, 关闭连接: {}", length, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // length 超过最大帧长度，防止恶意客户端构造超大 Length 导致 OOM
        if (length > maxFrameLength) {
            log.warn("帧长度 {} 超过上限 {}, 关闭连接: {}",
                    length, maxFrameLength, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // 剩余数据不够 Body 长度，回滚读指针等待（半包处理）
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }

        byte[] bodyBytes = new byte[length];
        in.readBytes(bodyBytes);

        MessageHeader header = new MessageHeader();
        header.setMagicNumber(magicNumber);
        header.setVersion(version);
        header.setSerializationType(serializationType);
        header.setMsgType(msgType);
        header.setSequenceId(sequenceId);
        header.setLength(length);

        MessagePacket packet = new MessagePacket();
        packet.setHeader(header);

        // 心跳消息直接按字符串处理，不走序列化
        if (MsgType.isHeartbeat(msgType)) {
            packet.setBody(new String(bodyBytes));
        } else if (msgType == MsgType.AUTH_REQ) {
            // 鉴权请求：反序列化为 AuthRequest 对象
            packet.setBody(Serializer.getSerializer(serializationType)
                    .deserialize(com.xsh.netty.protocol.AuthRequest.class, bodyBytes));
        } else if (msgType == MsgType.ACK) {
            // ACK 消息：body 为确认的 sequenceId 的字符串形式
            packet.setBody(new String(bodyBytes));
        } else {
            // 业务消息：保留原始字节数组，延迟到 MessageHandler 中按需反序列化
            packet.setRawBody(bodyBytes);
            packet.setBody(bodyBytes);
        }

        out.add(packet);
    }
}
