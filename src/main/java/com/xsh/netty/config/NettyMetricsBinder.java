package com.xsh.netty.config;

import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.PendingAckManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Netty 核心指标注册器，将关键运行指标暴露给 Micrometer/Prometheus。
 *
 * <p>注册的指标：
 * <ul>
 *   <li>netty.connections.active — 当前活跃连接数</li>
 *   <li>netty.connections.total — 历史总连接数</li>
 *   <li>netty.heartbeat.request.count — 心跳请求计数</li>
 *   <li>netty.heartbeat.response.count — 心跳响应计数</li>
 *   <li>netty.business.message.count — 业务消息计数</li>
 *   <li>netty.auth.success.count — 鉴权成功计数</li>
 *   <li>netty.auth.fail.count — 鉴权失败计数</li>
 *   <li>netty.ack.pending.count — 待确认消息数</li>
 *   <li>netty.business.message.latency — 消息处理延迟</li>
 * </ul>
 */
@Component
public class NettyMetricsBinder {

    private final Counter heartbeatReqCounter;
    private final Counter heartbeatRespCounter;
    private final Counter businessMsgCounter;
    private final Counter authSuccessCounter;
    private final Counter authFailCounter;
    private final Timer businessLatencyTimer;

    public NettyMetricsBinder(MeterRegistry registry,
                              DeviceChannelManager channelManager,
                              PendingAckManager pendingAckManager) {

        // Gauge：实时采集
        Gauge.builder("netty.connections.active", channelManager, DeviceChannelManager::getOnlineCount)
                .description("当前活跃连接数")
                .register(registry);

        Gauge.builder("netty.ack.pending", pendingAckManager, PendingAckManager::pendingCount)
                .description("待确认消息数")
                .register(registry);

        // Counter：累加计数
        heartbeatReqCounter = Counter.builder("netty.heartbeat.request")
                .description("心跳请求计数")
                .register(registry);

        heartbeatRespCounter = Counter.builder("netty.heartbeat.response")
                .description("心跳响应计数")
                .register(registry);

        businessMsgCounter = Counter.builder("netty.business.message")
                .description("业务消息计数")
                .register(registry);

        authSuccessCounter = Counter.builder("netty.auth.success")
                .description("鉴权成功计数")
                .register(registry);

        authFailCounter = Counter.builder("netty.auth.fail")
                .description("鉴权失败计数")
                .register(registry);

        // Timer：延迟统计
        businessLatencyTimer = Timer.builder("netty.business.message.latency")
                .description("业务消息处理延迟")
                .register(registry);
    }

    public void incrementHeartbeatReq() {
        heartbeatReqCounter.increment();
    }

    public void incrementHeartbeatResp() {
        heartbeatRespCounter.increment();
    }

    public void incrementBusinessMsg() {
        businessMsgCounter.increment();
    }

    public void incrementAuthSuccess() {
        authSuccessCounter.increment();
    }

    public void incrementAuthFail() {
        authFailCounter.increment();
    }

    /**
     * 记录业务消息处理延迟。
     */
    public void recordBusinessLatency(long durationNanos) {
        businessLatencyTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }
}
