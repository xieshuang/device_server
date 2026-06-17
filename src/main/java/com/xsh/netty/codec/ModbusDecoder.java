package com.xsh.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Modbus-TCP 协议解码器，解析 7 字节 MBAP 头部 + PDU 数据单元。
 *
 * <p>MBAP 头部结构（7字节）：
 * <pre>
 * ┌───────────────┬──────────────┬──────────┬─────────┐
 * │ TransactionId │  ProtocolId  │ Length   │ UnitId  │
 * │    (2B)       │ (2B = 0x0000)│  (2B)    │  (1B)   │
 * └───────────────┴──────────────┴──────────┴─────────┘
 * </pre>
 *
 * <p>Length 字段 = UnitId(1B) + PDU 长度，Modbus 规范限制不超过 256 字节。
 */
@Slf4j
public class ModbusDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // MBAP 头部至少需要 7 字节
        if (in.readableBytes() < 7) {
            return;
        }

        in.markReaderIndex();

        short transactionId = in.readShort();
        short protocolId = in.readShort();   // 必须为 0x0000

        if (protocolId != 0) {
            // 非标准 Modbus 协议，跳过此帧
            in.resetReaderIndex();
            ctx.close();
            log.warn("Modbus ProtocolId 异常: 0x{}", Integer.toHexString(protocolId & 0xFFFF));
            return;
        }

        int length = in.readUnsignedShort(); // 剩余字节数（UnitId + PDU）
        byte unitId = in.readByte();

        int pduLength = length - 1; // 减去 UnitId 的 1 字节
        if (pduLength < 0 || pduLength > 253) {
            log.warn("Modbus PDU 长度异常: {}", pduLength);
            in.resetReaderIndex();
            ctx.close();
            return;
        }

        // 半包检测
        if (in.readableBytes() < pduLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] pdu = new byte[pduLength];
        in.readBytes(pdu);

        ModbusFrame frame = new ModbusFrame();
        frame.setTransactionId(transactionId);
        frame.setUnitId(unitId);
        frame.setPdu(pdu);
        frame.setFunctionCode(pdu.length > 0 ? pdu[0] : 0);

        out.add(frame);
    }
}
