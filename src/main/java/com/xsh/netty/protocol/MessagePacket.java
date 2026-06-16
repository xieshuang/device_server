package com.xsh.netty.protocol;

import lombok.Data;

/**
 * 自定义协议消息包，包含头部和消息体。
 *
 * <p>心跳消息的 body 为 String 类型（如 "PING"/"PONG"），
 * 业务消息的 body 为经过序列化后的反序列化对象。
 */
@Data
public class MessagePacket {

    /** 消息头部，包含协议标识、版本、序列化方式、消息类型、长度等元信息 */
    private MessageHeader header;

    /** 消息体，类型取决于消息类型：心跳为 String，业务为反序列化对象 */
    private Object body;
}
