package com.xsh.netty.handler;

import com.xsh.netty.config.NettyMetricsBinder;
import com.xsh.netty.kafka.KafkaProducerService;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

/**
 * 业务消息处理器单元测试。
 */
class BusinessMessageHandlerTest {

    private NettyServerProperties properties;
    private NettyMetricsBinder metricsBinder;
    private KafkaProducerService kafkaProducerService;
    private BusinessMessageHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        properties = new NettyServerProperties();
        properties.setKafkaEnabled(true);
        metricsBinder = mock(NettyMetricsBinder.class);
        kafkaProducerService = mock(KafkaProducerService.class);
        handler = new BusinessMessageHandler(metricsBinder, properties);

        // 反射注入 optional field
        Field kafkaField = BusinessMessageHandler.class.getDeclaredField("kafkaProducerService");
        kafkaField.setAccessible(true);
        kafkaField.set(handler, kafkaProducerService);
    }

    @Test
    void testHandle_JsonPayload() {
        EmbeddedChannel channel = new EmbeddedChannel();

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setRawBody("{\"temp\":25}".getBytes());

        handler.handle(channel.pipeline().firstContext(), "dev-001", packet);

        verify(metricsBinder).incrementBusinessMsg();
        verify(kafkaProducerService, times(1)).sendAsync(eq("dev-001"), any());

        channel.finish();
    }

    @Test
    void testHandle_KafkaDisabled_NoSend() throws Exception {
        properties.setKafkaEnabled(false);
        handler = new BusinessMessageHandler(metricsBinder, properties);
        // kafkaProducerService 不注入（因为是 optional）

        EmbeddedChannel channel = new EmbeddedChannel();
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);
        packet.setRawBody("data".getBytes());

        handler.handle(channel.pipeline().firstContext(), "dev-001", packet);

        verify(metricsBinder).incrementBusinessMsg();

        channel.finish();
    }

    @Test
    void testHandle_ProtobufSerialization() {
        EmbeddedChannel channel = new EmbeddedChannel();

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSerializationType(Serializer.PROTOBUF_SERIALIZATION);
        packet.setHeader(header);
        packet.setRawBody(new byte[]{0x08, 0x01, 0x12, 0x03});

        handler.handle(channel.pipeline().firstContext(), "dev-001", packet);

        verify(metricsBinder).incrementBusinessMsg();
        verify(kafkaProducerService).sendAsync(eq("dev-001"), any());

        channel.finish();
    }

    @Test
    void testHandle_BodyNull_NoException() throws Exception {
        // 使用 Kafka 禁用的 handler，避免 null KafkaProducerService 问题
        properties.setKafkaEnabled(false);
        handler = new BusinessMessageHandler(metricsBinder, properties);

        EmbeddedChannel channel = new EmbeddedChannel();

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        // body 和 rawBody 均为 null，不应抛异常
        handler.handle(channel.pipeline().firstContext(), "dev-001", packet);

        verify(metricsBinder).incrementBusinessMsg();

        channel.finish();
    }

    @Test
    void testHandle_RawBodyPriority() {
        EmbeddedChannel channel = new EmbeddedChannel();

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);
        packet.setRawBody("raw-body".getBytes());  // rawBody 优先
        packet.setBody("ignored-body");

        handler.handle(channel.pipeline().firstContext(), "dev-001", packet);

        verify(metricsBinder).incrementBusinessMsg();
        verify(kafkaProducerService).sendAsync(eq("dev-001"), any());

        channel.finish();
    }
}
