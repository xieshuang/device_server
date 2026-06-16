package com.xsh.netty.protocol;

import lombok.Data;

/**
 * 自定义协议帧头部定义。
 *
 * <p>协议帧结构（V2，共15字节固定头部 + 变长Body）：
 * <pre>
 * +------------+--------+-----------+---------+-------------+-----------+---------+
 * | Magic(4B)  | Ver(1B)| Serial(1B)| Type(1B)| SeqId(4B)   | Length(4B)| Body(N) |
 * +------------+--------+-----------+---------+-------------+-----------+---------+
 * </pre>
 *
 * <ul>
 *   <li>Magic Number: 4字节，协议标识 0x44565352 ("DVSR")，用于区分协议类型</li>
 *   <li>Version: 1字节，协议版本号（当前=2），便于后续协议升级</li>
 *   <li>Serialization Type: 1字节，序列化方式（1=JSON, 2=Protobuf预留）</li>
 *   <li>Msg Type: 1字节，消息类型（1=心跳请求, 2=心跳响应, 3=业务数据, 4=鉴权请求, 5=鉴权成功, 6=鉴权失败, 7=ACK）</li>
 *   <li>Sequence ID: 4字节，消息序列号（0=不需要确认，>0=需要ACK确认）</li>
 *   <li>Length: 4字节，Body 的字节长度</li>
 *   <li>Body: 变长，实际业务数据</li>
 * </ul>
 *
 * <p>V1 兼容：V1 客户端发送的帧头部为11字节（无 sequenceId），Version=1。
 * V2 客户端发送的帧头部为15字节（含 sequenceId），Version=2。
 * 服务端根据 Version 字段选择不同的解码逻辑。
 */
@Data
public class MessageHeader {

    /** 协议魔数 "DVSR"，区别于 Java Class 文件的 0xCAFEBABE */
    public static final int MAGIC_NUMBER = 0x44565352;

    /** 协议魔数，默认 0x44565352 */
    private int magicNumber = MAGIC_NUMBER;

    /** 协议版本号，默认 2（V2 含 sequenceId） */
    private byte version = 2;

    /** 序列化方式：1=JSON, 2=Protobuf(预留) */
    private byte serializationType;

    /** 消息类型：1=心跳请求, 2=心跳响应, 3=业务数据, 4=鉴权请求, 5=鉴权成功, 6=鉴权失败, 7=ACK */
    private byte msgType;

    /** 消息序列号：0=不需要确认，>0=需要ACK确认 */
    private int sequenceId;

    /** Body 的字节长度 */
    private int length;

    /** V2 固定头部大小：magic(4) + version(1) + serializationType(1) + msgType(1) + sequenceId(4) + length(4) = 15 字节 */
    public static final int BASE_LENGTH = 15;

    /** V1 固定头部大小：magic(4) + version(1) + serializationType(1) + msgType(1) + length(4) = 11 字节 */
    public static final int V1_BASE_LENGTH = 11;
}
