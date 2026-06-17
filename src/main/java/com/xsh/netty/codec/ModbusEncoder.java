package com.xsh.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Modbus-TCP 协议编码器，将 ModbusFrame 封装为 7 字节 MBAP 头 + PDU 的二进制帧。
 */
public class ModbusEncoder extends MessageToByteEncoder<ModbusFrame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, ModbusFrame frame, ByteBuf out) throws Exception {
        byte[] pdu = frame.getPdu();
        int length = (pdu != null ? pdu.length : 0) + 1; // UnitId(1B) + PDU

        out.writeShort(frame.getTransactionId());  // TransactionId
        out.writeShort(0x0000);                      // ProtocolId = 0 (Modbus)
        out.writeShort(length);                      // Length
        out.writeByte(frame.getUnitId());            // UnitId
        if (pdu != null && pdu.length > 0) {
            out.writeBytes(pdu);                     // PDU
        }
    }
}
