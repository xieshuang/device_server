package com.xsh.netty.codec;

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
 *
 * <p>使用 Netty 的 {@link EmbeddedChannel} 模拟 Pipeline，
 * 测试编码器与解码器的正向/反向数据流转，以及异常场景的处理。
 */
class CustomProtocolCodecTest {

    private static final int MAX_FRAME_LENGTH = 10485760;

    /**
     * 测试心跳消息的编解码完整流程。
     * 编码器将 MessagePacket 编码为 ByteBuf，解码器再还原为 MessagePacket，
     * 验证心跳请求的类型和内容在编解码后保持一致。
     */
    @Test
    void testEncodeAndDecode_heartbeat() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolEncoder(),
                new CustomProtocolDecoder(MAX_FRAME_LENGTH)
        );

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.HEARTBEAT_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody("PING");

        // 写出（编码）
        assertTrue(channel.writeOutbound(packet));

        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);

        // 将编码后的字节写回（解码）
        assertTrue(channel.writeInbound(encoded));

        // 验证解码结果
        MessagePacket decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(MsgType.HEARTBEAT_REQ, decoded.getHeader().getMsgType());
        assertEquals("PING", decoded.getBody());
        assertEquals(MessageHeader.MAGIC_NUMBER, decoded.getHeader().getMagicNumber());

        channel.finish();
    }

    /**
     * 测试业务消息的编解码完整流程。
     * 验证业务数据的类型和序列化内容在编解码后保持一致。
     */
    @Test
    void testEncodeAndDecode_business() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolEncoder(),
                new CustomProtocolDecoder(MAX_FRAME_LENGTH)
        );

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody("test-business-data");

        assertTrue(channel.writeOutbound(packet));

        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(channel.writeInbound(encoded));

        MessagePacket decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(MsgType.BUSINESS, decoded.getHeader().getMsgType());
        assertEquals("test-business-data", decoded.getBody());

        channel.finish();
    }

    /**
     * 测试非法魔数时解码器关闭连接。
     * 魔数不匹配的自定义协议帧应直接断开，防止恶意攻击。
     */
    @Test
    void testDecode_invalidMagicNumber() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(MAX_FRAME_LENGTH)
        );

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(0xDEADBEEF); // 非法魔数
        buf.writeByte(1);         // version
        buf.writeByte(1);         // serialization
        buf.writeByte(1);         // msgType
        buf.writeInt(0);          // length

        channel.writeInbound(buf);

        // 非法魔数应导致连接关闭
        assertFalse(channel.isActive());

        channel.finish();
    }

    /**
     * 测试超过最大帧长度时解码器关闭连接。
     * length 字段超过 maxFrameLength 时应断开，防止恶意客户端导致 OOM。
     */
    @Test
    void testDecode_frameTooLarge() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(100) // 设置较小的帧上限
        );

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(MessageHeader.MAGIC_NUMBER);
        buf.writeByte(1);         // version
        buf.writeByte(1);         // serialization
        buf.writeByte(1);         // msgType
        buf.writeInt(200);        // length 超过上限（100）

        channel.writeInbound(buf);

        // 超大帧应导致连接关闭
        assertFalse(channel.isActive());

        channel.finish();
    }

    /**
     * 测试半包处理。
     * 数据不足一个完整协议帧时，解码器应等待更多数据，不产生输出，不关闭连接。
     */
    @Test
    void testDecode_partialPacket() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new CustomProtocolDecoder(MAX_FRAME_LENGTH)
        );

        // 仅发送部分头部（5字节，不足固定头部长度11字节）
        ByteBuf partial = Unpooled.buffer();
        partial.writeInt(MessageHeader.MAGIC_NUMBER);
        partial.writeByte(1);

        channel.writeInbound(partial);

        // 半包情况下不应产生输出，连接应保持活跃
        MessagePacket result = channel.readInbound();
        assertNull(result);
        assertTrue(channel.isActive());

        channel.finish();
    }
}
