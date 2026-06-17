package com.xsh.netty.handler;

import com.xsh.netty.codec.ModbusFrame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * Modbus-TCP 业务处理器，处理 Modbus 功能码请求并生成响应。
 *
 * <p>当前支持的 Modbus 功能码：
 * <ul>
 *   <li>0x01 — 读线圈（Read Coils）</li>
 *   <li>0x03 — 读保持寄存器（Read Holding Registers）</li>
 *   <li>0x04 — 读输入寄存器（Read Input Registers）</li>
 *   <li>0x06 — 写单个寄存器（Write Single Register）</li>
 *   <li>0x10 — 写多个寄存器（Write Multiple Registers）</li>
 * </ul>
 *
 * <p>非标准功能码回复异常码 0x01（非法功能）。
 *
 * <p>注意：此 Handler 为最简实现，生产环境应接入实际 I/O 数据源（PLC/传感器）。
 */
@Slf4j
public class ModbusBusinessHandler extends SimpleChannelInboundHandler<ModbusFrame> {

    /** Modbus 异常码：非法功能 */
    private static final byte EXCEPTION_ILLEGAL_FUNCTION = 0x01;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ModbusFrame frame) throws Exception {
        byte functionCode = frame.getFunctionCode();
        log.debug("Modbus 请求: unitId={}, functionCode=0x{}",
                frame.getUnitId(), Integer.toHexString(functionCode & 0xFF));

        switch (functionCode) {
            case 0x01: // 读线圈
            case 0x02: // 读离散输入
            case 0x03: // 读保持寄存器
            case 0x04: // 读输入寄存器
            case 0x06: // 写单个寄存器
            case 0x10: // 写多个寄存器
                // 标准功能码：回显相同数据（最简实现）
                ModbusFrame response = new ModbusFrame();
                response.setTransactionId(frame.getTransactionId());
                response.setUnitId(frame.getUnitId());
                response.setFunctionCode(functionCode);
                response.setPdu(frame.getPdu());
                ctx.writeAndFlush(response);
                break;
            default:
                // 非法功能码：返回异常响应
                sendExceptionResponse(ctx, frame, EXCEPTION_ILLEGAL_FUNCTION);
                break;
        }
    }

    /**
     * 发送 Modbus 异常响应。
     * 异常响应格式：功能码 | 0x80 + 异常码
     */
    private void sendExceptionResponse(ChannelHandlerContext ctx, ModbusFrame request, byte exceptionCode) {
        ModbusFrame response = new ModbusFrame();
        response.setTransactionId(request.getTransactionId());
        response.setUnitId(request.getUnitId());
        byte[] pdu = new byte[2];
        pdu[0] = (byte) (request.getFunctionCode() | 0x80); // 功能码最高位置 1
        pdu[1] = exceptionCode;
        response.setPdu(pdu);
        response.setFunctionCode(pdu[0]);
        ctx.writeAndFlush(response);
        log.warn("Modbus 非法功能码: unitId={}, functionCode=0x{}",
                request.getUnitId(), Integer.toHexString(request.getFunctionCode() & 0xFF));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Modbus 处理异常: {}", cause.getMessage());
        ctx.close();
    }
}
