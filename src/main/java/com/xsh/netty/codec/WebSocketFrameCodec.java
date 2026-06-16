package com.xsh.netty.codec;

import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

/**
 * WebSocket 帧 ↔ MessagePacket 转换器。
 *
 * <p>入站方向：BinaryWebSocketFrame → MessagePacket
 * <p>出站方向：MessagePacket → BinaryWebSocketFrame
 *
 * <p>WebSocket 二进制帧体格式（简化版，省略 Magic 和 Version）：
 * <pre>
 * serializationType(1B) + msgType(1B) + sequenceId(4B) + payload(NB)
 * </pre>
 *
 * <p>设计说明：WebSocket 承载的消息不包含 Magic 和 Version 字段，
 * 因为 WebSocket 连接已通过协议升级确认了通道类型，无需再标识协议。
 */
public class WebSocketFrameCodec {

    private WebSocketFrameCodec() {}

    /**
     * 将 BinaryWebSocketFrame 解码为 MessagePacket。
     *
     * @param buf WebSocket 帧内容
     * @return 解码后的消息包
     */
    public static MessagePacket decode(ByteBuf buf) {
        // 至少需要 6 字节的头部（serializationType + msgType + sequenceId）
        if (buf.readableBytes() < 6) {
            return null;
        }

        byte serializationType = buf.readByte();
        byte msgType = buf.readByte();
        int sequenceId = buf.readInt();

        // 剩余字节为 payload
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);

        // 构造 MessagePacket
        MessageHeader header = new MessageHeader();
        header.setMagicNumber(MessageHeader.MAGIC_NUMBER);
        header.setVersion((byte) 2); // WebSocket 默认使用 V2
        header.setSerializationType(serializationType);
        header.setMsgType(msgType);
        header.setSequenceId(sequenceId);
        header.setLength(payload.length);

        MessagePacket packet = new MessagePacket();
        packet.setHeader(header);
        packet.setBody(payload);
        packet.setRawBody(payload);

        return packet;
    }

    /**
     * 将 MessagePacket 编码为 BinaryWebSocketFrame。
     *
     * @param packet 消息包
     * @return WebSocket 二进制帧
     */
    public static BinaryWebSocketFrame encode(MessagePacket packet) {
        MessageHeader header = packet.getHeader();
        int payloadLength = packet.getRawBody() != null ? packet.getRawBody().length : 0;
        ByteBuf buf = Unpooled.buffer(6 + payloadLength);

        buf.writeByte(header.getSerializationType());
        buf.writeByte(header.getMsgType());
        buf.writeInt(header.getSequenceId());

        if (packet.getRawBody() != null) {
            buf.writeBytes(packet.getRawBody());
        } else if (packet.getBody() instanceof byte[] bytes) {
            buf.writeBytes(bytes);
        } else if (packet.getBody() instanceof String s) {
            buf.writeBytes(s.getBytes());
        }

        return new BinaryWebSocketFrame(buf);
    }
}
