package com.xsh.netty.codec;

import com.xsh.netty.protocol.AuthRequest;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 自定义协议编解码器单元测试。
 */
class CustomProtocolCodecTest {

    private static final int MAX_FRAME_LENGTH = 10485760;

    // ==================== V2 编解码往返 ====================

    @Test
    void testEncodeAndDecode_heartbeat_V2() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolEncoder(), new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.HEARTBEAT_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        header.setSequenceId(0);
        packet.setHeader(header);
        packet.setBody("PING");

        assertTrue(channel.writeOutbound(packet));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(channel.writeInbound(encoded));

        MessagePacket decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(MsgType.HEARTBEAT_REQ, decoded.getHeader().getMsgType());
        assertEquals("PING", decoded.getBody());
        assertEquals(0, decoded.getHeader().getSequenceId());

        channel.finish();
    }

    @Test
    void testEncodeAndDecode_business_V2() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolEncoder(), new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        header.setSequenceId(100);
        packet.setHeader(header);
        packet.setBody("test-data");

        assertTrue(channel.writeOutbound(packet));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(channel.writeInbound(encoded));

        MessagePacket decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(MsgType.BUSINESS, decoded.getHeader().getMsgType());
        assertEquals(100, decoded.getHeader().getSequenceId());

        channel.finish();
    }

    // ==================== V1 向后兼容 ====================

    @Test
    void testDecode_V1_BackwardCompat() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        // 手动构造 V1 帧（11字节头部，无 sequenceId）
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(MessageHeader.MAGIC_NUMBER);
        buf.writeByte(1);  // version = 1 (V1)
        buf.writeByte(1);  // serializationType
        buf.writeByte(1);  // msgType = 心跳请求
        buf.writeInt(4);   // length = 4 ("PING")

        channel.writeInbound(buf);

        MessagePacket decoded = channel.readInbound();
        assertNull(decoded); // 半包（没有 body 字节），等待更多数据
        assertTrue(channel.isActive());

        // 补充 body 字节
        ByteBuf body = Unpooled.buffer();
        body.writeBytes("PING".getBytes());
        channel.writeInbound(body);

        decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(1, decoded.getHeader().getVersion());
        assertEquals(0, decoded.getHeader().getSequenceId()); // V1 默认 0

        channel.finish();
    }

    // ==================== 异常场景 ====================

    @Test
    void testDecode_InvalidMagic_Close() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(0xDEADBEEF);
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeInt(0);

        channel.writeInbound(buf);
        assertFalse(channel.isActive());
        channel.finish();
    }

    @Test
    void testDecode_FrameTooLarge_Close() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(100));

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(MessageHeader.MAGIC_NUMBER);
        buf.writeByte(2);  // V2
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeInt(0);
        buf.writeInt(200); // length 超过 maxFrameLength

        channel.writeInbound(buf);
        assertFalse(channel.isActive());
        channel.finish();
    }

    @Test
    void testDecode_NegativeLength_Close() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(MessageHeader.MAGIC_NUMBER);
        buf.writeByte(2);
        buf.writeByte(1);
        buf.writeByte(1);
        buf.writeInt(0);
        buf.writeInt(-1); // 负数长度

        channel.writeInbound(buf);
        assertFalse(channel.isActive());
        channel.finish();
    }

    @Test
    void testDecode_PartialPacket_Wait() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        // 仅 7 字节 V2 头部初始字段，不足完整 15 字节
        ByteBuf partial = Unpooled.buffer();
        partial.writeInt(MessageHeader.MAGIC_NUMBER);
        partial.writeByte(2);
        partial.writeByte(1);
        partial.writeByte(1);

        channel.writeInbound(partial);
        MessagePacket result = channel.readInbound();
        assertNull(result);
        assertTrue(channel.isActive());
        channel.finish();
    }

    // ==================== 特定消息类型编解码 ====================

    @Test
    void testEncode_AuthRequest() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolEncoder(), new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        AuthRequest authReq = new AuthRequest();
        authReq.setDeviceId("dev-001");
        authReq.setTimestamp(System.currentTimeMillis());
        authReq.setToken("token123");
        packet.setBody(authReq);

        assertTrue(channel.writeOutbound(packet));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(channel.writeInbound(encoded));

        MessagePacket decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(MsgType.AUTH_REQ, decoded.getHeader().getMsgType());
        assertTrue(decoded.getBody() instanceof AuthRequest);
        assertEquals("dev-001", ((AuthRequest) decoded.getBody()).getDeviceId());

        channel.finish();
    }

    @Test
    void testEncode_ACK() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolEncoder(), new CustomProtocolDecoder(MAX_FRAME_LENGTH));

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.ACK);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        header.setSequenceId(999);
        packet.setHeader(header);
        packet.setBody("999");

        assertTrue(channel.writeOutbound(packet));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(channel.writeInbound(encoded));

        MessagePacket decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(MsgType.ACK, decoded.getHeader().getMsgType());
        assertEquals(999, decoded.getHeader().getSequenceId());

        channel.finish();
    }
}
