package com.xsh.netty.handler;

import com.xsh.netty.config.NettyMetricsBinder;
import com.xsh.netty.dispatcher.MessageHandler;
import com.xsh.netty.kafka.KafkaMessageEnvelope;
import com.xsh.netty.kafka.KafkaProducerService;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * 业务消息处理器，处理 MsgType.BUSINESS 类型的消息。
 *
 * <p>核心流程：
 * <ol>
 *   <li>根据 serializationType 区分处理路径（JSON/Protobuf）</li>
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

    /** Kafka 发送服务（kafka-enabled=false 时为 null） */
    @Autowired(required = false)
    private KafkaProducerService kafkaProducerService;

    private final NettyMetricsBinder metricsBinder;
    private final NettyServerProperties properties;

    public BusinessMessageHandler(NettyMetricsBinder metricsBinder,
                                   NettyServerProperties properties) {
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

        if (properties.isKafkaEnabled() && kafkaProducerService != null) {
            // 1. 构造 Kafka 消息信封的 Header 元信息
            Map<String, String> headers = new HashMap<>();
            headers.put("msgType", String.valueOf(packet.getHeader().getMsgType()));
            headers.put("deviceId", deviceId);
            headers.put("serializationType", String.valueOf(packet.getHeader().getSerializationType()));
            headers.put("version", String.valueOf(packet.getHeader().getVersion()));
            headers.put("sequenceId", String.valueOf(packet.getHeader().getSequenceId()));

            // 2. 按序列化类型处理 payload
            String payload;
            byte[] rawBytes = resolveRawBytes(packet);
            if (packet.getHeader().getSerializationType() == Serializer.PROTOBUF_SERIALIZATION) {
                // Protobuf 二进制数据 → Base64 编码，避免 UTF-8 转码损坏数据
                payload = rawBytes != null ? Base64.getEncoder().encodeToString(rawBytes) : "";
            } else {
                // JSON / 心跳等文本数据 → UTF-8 字符串
                payload = rawBytes != null ? new String(rawBytes, StandardCharsets.UTF_8) : "";
            }

            // 3. 构造信封并异步发送
            KafkaMessageEnvelope envelope = KafkaMessageEnvelope.builder()
                    .headers(headers)
                    .payload(payload)
                    .receivedAt(System.currentTimeMillis())
                    .build();
            kafkaProducerService.sendAsync(deviceId, envelope);
        } else {
            // Kafka 未启用，仅打印日志（注意不打印 body 敏感数据）
            log.info("业务消息(Kafka未启用): deviceId={}, seqId={}", deviceId,
                    packet.getHeader().getSequenceId());
        }

        // 4. 记录指标
        metricsBinder.incrementBusinessMsg();
        metricsBinder.recordBusinessLatency(System.nanoTime() - startNanos);

        log.debug("业务消息已处理: deviceId={}, seqId={}", deviceId,
                packet.getHeader().getSequenceId());
    }

    /**
     * 从 MessagePacket 中提取原始字节数组。
     * 优先 rawBody（解码器直接存入），其次 body（byte[] 类型），最后为空。
     */
    private byte[] resolveRawBytes(MessagePacket packet) {
        if (packet.getRawBody() != null) {
            return packet.getRawBody();
        }
        if (packet.getBody() instanceof byte[] bytes) {
            return bytes;
        }
        return null;
    }
}
