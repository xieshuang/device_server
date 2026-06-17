package com.xsh.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Modbus-TCP 编解码器单元测试。
 *
 * <p>覆盖 MBAP 头部编解码往返、非法 ProtocolId、异常 PDU 长度、半包处理。
 */
class ModbusCodecTest {

    // ==================== 正常编解码往返 ====================

    @Test
    void testEncodeDecode_ReadCoils() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusEncoder(), new ModbusDecoder());

        ModbusFrame frame = new ModbusFrame();
        frame.setTransactionId((short) 1);
        frame.setUnitId((byte) 1);
        frame.setFunctionCode((byte) 0x01); // 读线圈
        frame.setPdu(new byte[]{0x01, 0x00, 0x00, 0x00, 0x0A}); // 功能码+起始地址+数量

        assertTrue(channel.writeOutbound(frame));
        ByteBuf encoded = channel.readOutbound();
        assertNotNull(encoded);
        assertTrue(channel.writeInbound(encoded));

        ModbusFrame decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(1, decoded.getTransactionId());
        assertEquals(1, decoded.getUnitId());
        assertEquals(0x01, decoded.getFunctionCode());
        assertArrayEquals(new byte[]{0x01, 0x00, 0x00, 0x00, 0x0A}, decoded.getPdu());

        channel.finish();
    }

    @Test
    void testEncodeDecode_WriteSingleRegister() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusEncoder(), new ModbusDecoder());

        ModbusFrame frame = new ModbusFrame();
        frame.setTransactionId((short) 2);
        frame.setUnitId((byte) 2);
        frame.setFunctionCode((byte) 0x06);
        frame.setPdu(new byte[]{0x06, 0x00, 0x01, 0x00, 0x2A}); // 写单个寄存器

        assertTrue(channel.writeOutbound(frame));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));

        ModbusFrame decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(2, decoded.getTransactionId());
        assertEquals(0x06, decoded.getFunctionCode());

        channel.finish();
    }

    @Test
    void testEncodeDecode_EmptyPdu() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusEncoder(), new ModbusDecoder());

        ModbusFrame frame = new ModbusFrame();
        frame.setTransactionId((short) 3);
        frame.setUnitId((byte) 1);
        frame.setPdu(new byte[0]);

        assertTrue(channel.writeOutbound(frame));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));

        ModbusFrame decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(0, decoded.getPdu().length);

        channel.finish();
    }

    // ==================== 异常场景 ====================

    @Test
    void testDecode_NonZeroProtocolId_Close() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusDecoder());

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(1);     // transactionId
        buf.writeShort(0xABCD); // ProtocolId != 0（非法）
        buf.writeShort(2);     // length
        buf.writeByte(1);      // unitId
        buf.writeByte(0x01);   // 假 PDU

        channel.writeInbound(buf);
        assertFalse(channel.isActive());
        channel.finish();
    }

    @Test
    void testDecode_NegativePduLength_Close() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusDecoder());

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(1);
        buf.writeShort(0x0000); // ProtocolId = 0
        buf.writeShort(0);       // length = 0 → PDU 长度 = -1
        buf.writeByte(1);

        channel.writeInbound(buf);
        assertFalse(channel.isActive());
        channel.finish();
    }

    @Test
    void testDecode_PartialPacket_Wait() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusDecoder());

        // 仅发送部分 MBAP 头（6字节，不足7字节）
        ByteBuf partial = Unpooled.buffer();
        partial.writeShort(1);
        partial.writeShort(0x0000);
        partial.writeShort(5); // length = 5, need 1 unitId + 4 pdu = 5 more bytes

        channel.writeInbound(partial);
        ModbusFrame result = channel.readInbound();
        assertNull(result); // 半包，等待更多数据
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test
    void testEncodeDecode_RoundTrip_LargeTransactionId() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new ModbusEncoder(), new ModbusDecoder());

        ModbusFrame frame = new ModbusFrame();
        frame.setTransactionId(Short.MAX_VALUE);
        frame.setUnitId((byte) 0xFF);
        frame.setFunctionCode((byte) 0x10);
        frame.setPdu(new byte[]{0x10, 0x00, 0x01, 0x00, 0x02, 0x04, 0x00, 0x0A, 0x00, 0x14});

        assertTrue(channel.writeOutbound(frame));
        ByteBuf encoded = channel.readOutbound();
        assertTrue(channel.writeInbound(encoded));

        ModbusFrame decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(Short.MAX_VALUE, decoded.getTransactionId());
        assertEquals((byte) 0xFF, decoded.getUnitId());
        assertEquals(0x10, decoded.getFunctionCode());

        channel.finish();
    }
}
