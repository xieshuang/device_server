package com.xsh.netty.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Kafka 消息信封，统一 Topic 中所有消息的外层格式。
 *
 * <p>设计原则：统一 Topic + Header 区分消息类型 + deviceId 分区保证有序。
 *
 * <p>Header 字段说明：
 * <ul>
 *   <li>msgType: 消息类型（3=业务, 7=ACK 等）</li>
 *   <li>deviceId: 设备ID，用于分区路由</li>
 *   <li>serializationType: 序列化方式（1=JSON, 2=Protobuf）</li>
 *   <li>version: 协议版本（1 或 2）</li>
 *   <li>sequenceId: 消息序列号</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessageEnvelope {

    /** 消息头部元信息，用于消费者按需过滤和路由 */
    private Map<String, String> headers;

    /** 消息体（JSON 字符串或 Base64 编码的二进制数据） */
    private String payload;

    /** 服务端接收时间戳（毫秒） */
    private long receivedAt;
}
