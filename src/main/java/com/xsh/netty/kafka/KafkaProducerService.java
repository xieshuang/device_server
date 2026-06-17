package com.xsh.netty.kafka;

import com.xsh.netty.config.NettyMetricsBinder;
import com.xsh.netty.server.NettyServerProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;

/**
 * Kafka 消息发送服务，封装 Producer 发送逻辑。
 *
 * <p>核心设计：
 * <ul>
 *   <li>按 deviceId hash 选择分区，保证同一设备消息有序</li>
 *   <li>异步发送 + 回调，不阻塞 Netty EventLoop 线程</li>
 *   <li>发送失败记录日志和指标，Kafka 自身有重试机制</li>
 * </ul>
 *
 * <p>线程安全：KafkaProducer 是线程安全的，多 EventLoop 共享无问题。
 */
@Slf4j
@Service
@ConditionalOnProperty(name = "netty.server.kafka-enabled", havingValue = "true")
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NettyMetricsBinder metricsBinder;
    private final NettyServerProperties properties;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate,
                                 NettyMetricsBinder metricsBinder,
                                 NettyServerProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.metricsBinder = metricsBinder;
        this.properties = properties;
    }

    /**
     * 异步发送消息到 Kafka。
     *
     * @param deviceId 设备ID，用作消息 key 进行分区路由
     * @param envelope 消息信封
     */
    public void sendAsync(String deviceId, KafkaMessageEnvelope envelope) {
        // 构造 Kafka Header，携带消息元信息供消费者过滤
        Headers kafkaHeaders = new RecordHeaders();
        envelope.getHeaders().forEach((key, value) ->
                kafkaHeaders.add(key, value.getBytes(StandardCharsets.UTF_8)));

        // 构造 ProducerRecord，key=deviceId 保证同一设备消息路由到同一分区
        ProducerRecord<String, String> record = new ProducerRecord<>(
                properties.getKafkaTopic(),
                null, // partition 由 Kafka 根据 key 的 hash 自动选择
                deviceId, // key = deviceId，按 hash 分区保证有序
                envelope.getPayload(),
                kafkaHeaders
        );

        // 异步发送，回调中记录指标和日志
        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Kafka 发送失败: deviceId={}", deviceId, ex);
                metricsBinder.incrementKafkaSendFail();
            } else {
                log.debug("Kafka 发送成功: deviceId={}, partition={}",
                        deviceId, result.getRecordMetadata().partition());
                metricsBinder.incrementKafkaSendSuccess();
            }
        });
    }
}
