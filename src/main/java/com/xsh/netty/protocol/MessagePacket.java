package com.xsh.netty.protocol;

import lombok.Data;

/**
 * 自定义协议消息包，包含头部和消息体。
 *
 * <p>心跳消息的 body 为 String 类型（如 "PING"/"PONG"），
 * 业务消息的 body 为原始字节数组（延迟反序列化到 MessageHandler 中按需执行），
 * 鉴权消息的 body 为 AuthRequest 对象。
 */
@Data
public class MessagePacket {

    /** 消息头部，包含协议标识、版本、序列化方式、消息类型、长度等元信息 */
    private MessageHeader header;

    /** 消息体，类型取决于消息类型：心跳为 String，鉴权为 AuthRequest，业务为 byte[] */
    private Object body;

    /**
     * 原始消息体字节数组，用于业务消息延迟反序列化。
     *
     * <p>解码器将业务消息的原始字节存入此字段，
     * 由 {@link com.xsh.netty.dispatcher.MessageHandler} 实现中按需调用
     * {@link com.xsh.netty.serialize.Serializer#deserialize} 反序列化为具体 DTO。
     */
    private byte[] rawBody;

    /** V5 全链路追踪ID，解码时生成，贯穿编解码→鉴权→Dispatcher→Kafka */
    private String traceId;
}
