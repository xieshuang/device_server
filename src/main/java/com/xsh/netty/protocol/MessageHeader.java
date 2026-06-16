package com.xsh.netty.protocol;

import lombok.Data;

/**
 * 自定义协议帧头部定义。
 *
 * <p>协议帧结构（共11字节固定头部 + 变长Body）：
 * <pre>
 * +------------+--------+-----------+---------+-----------+---------+
 * | Magic(4B)  | Ver(1B)| Serial(1B)| Type(1B)| Length(4B)| Body(N) |
 * +------------+--------+-----------+---------+-----------+---------+
 * </pre>
 *
 * <ul>
 *   <li>Magic Number: 4字节，协议标识 0x44565352 ("DVSR")，用于区分协议类型</li>
 *   <li>Version: 1字节，协议版本号，便于后续协议升级</li>
 *   <li>Serialization Type: 1字节，序列化方式（1=JSON, 2=Protobuf预留）</li>
 *   <li>Msg Type: 1字节，消息类型（1=心跳请求, 2=心跳响应, 3=业务数据）</li>
 *   <li>Length: 4字节，Body 的字节长度</li>
 *   <li>Body: 变长，实际业务数据</li>
 * </ul>
 */
@Data
public class MessageHeader {

    /** 协议魔数 "DVSR"，区别于 Java Class 文件的 0xCAFEBABE */
    public static final int MAGIC_NUMBER = 0x44565352;

    /** 协议魔数，默认 0x44565352 */
    private int magicNumber = MAGIC_NUMBER;

    /** 协议版本号，默认 1 */
    private byte version = 1;

    /** 序列化方式：1=JSON, 2=Protobuf(预留) */
    private byte serializationType;

    /** 消息类型：1=心跳请求, 2=心跳响应, 3=业务数据 */
    private byte msgType;

    /** Body 的字节长度 */
    private int length;

    /** 固定头部大小：magic(4) + version(1) + serializationType(1) + msgType(1) + length(4) = 11 字节 */
    public static final int BASE_LENGTH = 11;
}
