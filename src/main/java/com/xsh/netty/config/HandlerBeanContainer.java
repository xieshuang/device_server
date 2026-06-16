package com.xsh.netty.config;

import com.xsh.netty.dispatcher.MessageDispatcher;
import com.xsh.netty.kafka.KafkaProducerService;
import com.xsh.netty.ratelimit.RateLimiterService;
import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.NettyServerProperties;
import com.xsh.netty.server.PendingAckManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Handler 依赖容器，解决 Netty Handler 无法直接注入 Spring Bean 的问题。
 *
 * <p>Handler 在 {@link io.netty.channel.ChannelInitializer#initChannel} 中通过 new 创建，
 * 不在 Spring 容器管理范围内。通过此容器集中持有所有 Handler 需要的 Bean 引用，
 * Handler 构造时传入即可访问 Spring 管理的服务。
 *
 * <p>线程安全：所有字段均为线程安全的 Spring Bean（@Component/@Service），
 * Netty EventLoop 线程通过引用访问不会产生并发问题。
 */
@Getter
@Component
@RequiredArgsConstructor
public class HandlerBeanContainer {

    /** 消息分发器，根据 msgType 路由消息到对应处理器 */
    private final MessageDispatcher messageDispatcher;

    /** 指标绑定器，记录 Micrometer/Prometheus 指标 */
    private final NettyMetricsBinder metricsBinder;

    /** 待确认消息管理器，管理需要 ACK 的消息 */
    private final PendingAckManager pendingAckManager;

    /** 设备连接管理器，维护 deviceId → Channel 映射 */
    private final DeviceChannelManager channelManager;

    /** Kafka 消息发送服务，业务消息异步持久化 */
    private final KafkaProducerService kafkaProducerService;

    /** 限流服务，全局+单设备双维度令牌桶 */
    private final RateLimiterService rateLimiterService;

    /** 服务端配置属性 */
    private final NettyServerProperties properties;
}
