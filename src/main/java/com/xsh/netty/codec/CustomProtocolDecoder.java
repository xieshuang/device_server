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
 * <p>处理流程：
 * <ol>
 *   <li>检查可读字节是否达到固定头部长度（11字节），不足则等待</li>
 *   <li>标记读指针，校验魔数，非法则直接关闭连接</li>
 *   <li>读取头部字段，校验 length 合法性（非负且不超过 maxFrameLength）</li>
 *   <li>检查剩余可读字节是否足够 Body 长度，不足则回滚读指针等待（粘包/半包处理）</li>
 *   <li>根据消息类型解码 Body：心跳直接转 String，业务数据走序列化器反序列化</li>
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
        // 可读字节不足固定头部长度，等待更多数据到达
        if (in.readableBytes() < MessageHeader.BASE_LENGTH) {
            return;
        }

        // 标记读指针，用于半包回滚
        in.markReaderIndex();

        // 校验魔数，非法协议直接断开，防止恶意攻击
        int magicNumber = in.readInt();
        if (magicNumber != MessageHeader.MAGIC_NUMBER) {
            log.warn("Invalid magic number: 0x{}, closing channel: {}",
                    Integer.toHexString(magicNumber), ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        byte version = in.readByte();
        byte serializationType = in.readByte();
        byte msgType = in.readByte();
        int length = in.readInt();

        // length 为负数，非法数据，断开连接
        if (length < 0) {
            log.warn("Negative length: {}, closing channel: {}", length, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // length 超过最大帧长度，防止恶意客户端构造超大 Length 导致 OOM
        if (length > maxFrameLength) {
            log.warn("Frame length {} exceeds max {}, closing channel: {}",
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
        header.setLength(length);

        MessagePacket packet = new MessagePacket();
        packet.setHeader(header);

        // 心跳消息直接按字符串处理，不走序列化
        if (MsgType.isHeartbeat(msgType)) {
            packet.setBody(new String(bodyBytes));
        } else {
            // 业务消息根据 serializationType 选择序列化器进行反序列化
            packet.setBody(Serializer.getSerializer(serializationType).deserialize(String.class, bodyBytes));
        }

        out.add(packet);
    }
}
