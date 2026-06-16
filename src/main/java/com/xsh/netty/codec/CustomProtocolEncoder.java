package com.xsh.netty.codec;

import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

/**
 * 自定义协议编码器，将 {@link MessagePacket} 按 V2 协议帧格式写入 {@link ByteBuf}。
 *
 * <p>V2 编码顺序：Magic(4B) → Version(1B) → SerializationType(1B) → MsgType(1B) → SequenceId(4B) → Length(4B) → Body(NB)
 *
 * <p>注意：Netty 的 {@link MessageToByteEncoder} 会自动管理 ByteBuf 的释放，
 * 业务代码中无需手动调用 ReferenceCountUtil.release()。
 */
@Slf4j
public class CustomProtocolEncoder extends MessageToByteEncoder<MessagePacket> {

    @Override
    protected void encode(ChannelHandlerContext ctx, MessagePacket packet, ByteBuf out) throws Exception {
        MessageHeader header = packet.getHeader();

        // 写入固定头部
        out.writeInt(header.getMagicNumber());
        out.writeByte(header.getVersion());
        out.writeByte(header.getSerializationType());
        out.writeByte(header.getMsgType());
        out.writeInt(header.getSequenceId());

        // 根据消息类型编码 Body
        byte[] bodyBytes;
        if (packet.getBody() == null) {
            bodyBytes = new byte[0];
        } else if (MsgType.isHeartbeat(header.getMsgType())) {
            // 心跳消息直接按字符串编码，不走序列化
            bodyBytes = packet.getBody().toString().getBytes();
        } else {
            // 业务/鉴权消息根据 serializationType 选择序列化器
            bodyBytes = Serializer.getSerializer(header.getSerializationType()).serialize(packet.getBody());
        }

        // 写入 Body 长度和 Body 数据
        out.writeInt(bodyBytes.length);
        out.writeBytes(bodyBytes);
    }
}
