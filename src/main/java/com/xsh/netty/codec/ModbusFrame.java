package com.xsh.netty.codec;

import lombok.Data;

/**
 * Modbus-TCP 数据帧，封装 MBAP 头 + PDU。
 */
@Data
public class ModbusFrame {

    /** 事务标识符，客户端生成，服务端原样返回 */
    private short transactionId;

    /** 单元标识符（从站地址） */
    private byte unitId;

    /** 功能码（PDU 首字节） */
    private byte functionCode;

    /** PDU 数据（不含 MBAP 头） */
    private byte[] pdu;
}
