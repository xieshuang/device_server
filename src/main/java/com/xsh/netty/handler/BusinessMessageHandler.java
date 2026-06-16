package com.xsh.netty.handler;

import com.xsh.netty.config.NettyMetricsBinder;
import com.xsh.netty.dispatcher.MessageHandler;
import com.xsh.netty.kafka.KafkaMessageEnvelope;
import com.xsh.netty.kafka.KafkaProducerService;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 业务消息处理器，处理 MsgType.BUSINESS 类型的消息。
 *
 * <p>核心流程：
 * <ol>
 *   <li>构造 Kafka 消息信封</li>
 *   <li>异步写入 Kafka（kafkaEnabled=true 时），否则仅打印日志</li>
 *   <li>记录业务消息指标</li>
 * </ol>
 *
 * <p>延迟反序列化：此 Handler 从 {@link MessagePacket#getRawBody()} 获取原始字节，
 * 直接透传到 Kafka，无需在服务端反序列化为具体 DTO，降低 CPU 开销。
 */
@Slf4j
@Component
public class BusinessMessageHandler implements MessageHandler {

    private final KafkaProducerService kafkaProducerService;
    private final NettyMetricsBinder metricsBinder;
    private final NettyServerProperties properties;

    public BusinessMessageHandler(KafkaProducerService kafkaProducerService,
                                   NettyMetricsBinder metricsBinder,
                                   NettyServerProperties properties) {
        this.kafkaProducerService = kafkaProducerService;
        this.metricsBinder = metricsBinder;
        this.properties = properties;
    }

    @Override
    public byte supportMsgType() {
        return MsgType.BUSINESS;
    }

    @Override
    public void handle(ChannelHandlerContext ctx, String deviceId, MessagePacket packet) {
        long startNanos = System.nanoTime();

        if (properties.isKafkaEnabled()) {
            // 1. 构造 Kafka 消息信封的 Header 元信息
            Map<String, String> headers = new HashMap<>();
            headers.put("msgType", String.valueOf(packet.getHeader().getMsgType()));
            headers.put("deviceId", deviceId);
            headers.put("serializationType", String.valueOf(packet.getHeader().getSerializationType()));
            headers.put("version", String.valueOf(packet.getHeader().getVersion()));
            headers.put("sequenceId", String.valueOf(packet.getHeader().getSequenceId()));

            // 2. 序列化 payload：原始字节直接转 UTF-8 字符串
            String payload;
            if (packet.getRawBody() != null) {
                payload = new String(packet.getRawBody(), StandardCharsets.UTF_8);
            } else if (packet.getBody() instanceof byte[] bytes) {
                payload = new String(bytes, StandardCharsets.UTF_8);
            } else {
                payload = packet.getBody().toString();
            }

            // 3. 构造信封并异步发送
            KafkaMessageEnvelope envelope = KafkaMessageEnvelope.builder()
                    .headers(headers)
                    .payload(payload)
                    .receivedAt(System.currentTimeMillis())
                    .build();
            kafkaProducerService.sendAsync(deviceId, envelope);
        } else {
            // Kafka 未启用，仅打印日志
            log.info("业务消息(Kafka未启用): deviceId={}, body={}", deviceId, packet.getBody());
        }

        // 4. 记录指标
        metricsBinder.incrementBusinessMsg();
        metricsBinder.recordBusinessLatency(System.nanoTime() - startNanos);

        log.debug("业务消息已处理: deviceId={}, seqId={}", deviceId,
                packet.getHeader().getSequenceId());
    }
}
