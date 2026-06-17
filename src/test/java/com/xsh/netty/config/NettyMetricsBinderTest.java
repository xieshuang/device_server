package com.xsh.netty.auth;

import com.xsh.netty.config.NettyMetricsBinder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * NettyMetricsBinder 单元测试。
 */
class NettyMetricsBinderTest {

    private MeterRegistry registry;
    private NettyMetricsBinder binder;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();

        // Mock dependencies
        com.xsh.netty.server.DeviceChannelManager channelManager =
                mock(com.xsh.netty.server.DeviceChannelManager.class);
        when(channelManager.getOnlineCount()).thenReturn(5);

        com.xsh.netty.server.PendingAckManager pendingAckManager =
                mock(com.xsh.netty.server.PendingAckManager.class);
        when(pendingAckManager.pendingCount()).thenReturn(10L);

        binder = new NettyMetricsBinder(registry, channelManager, pendingAckManager);
    }

    @Test
    void testIncrementHeartbeatReq() {
        binder.incrementHeartbeatReq();
        double count = registry.get("netty.heartbeat.request").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementHeartbeatResp() {
        binder.incrementHeartbeatResp();
        double count = registry.get("netty.heartbeat.response").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementBusinessMsg() {
        binder.incrementBusinessMsg();
        double count = registry.get("netty.business.message").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementAuthSuccess() {
        binder.incrementAuthSuccess();
        double count = registry.get("netty.auth.success").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementAuthFail() {
        binder.incrementAuthFail();
        double count = registry.get("netty.auth.fail").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testRecordBusinessLatency() {
        binder.recordBusinessLatency(1_000_000L); // 1ms in nanos
        double count = registry.get("netty.business.message.latency").timer().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementKafkaSendSuccess() {
        binder.incrementKafkaSendSuccess();
        double count = registry.get("netty.kafka.send.success").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementKafkaSendFail() {
        binder.incrementKafkaSendFail();
        double count = registry.get("netty.kafka.send.fail").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testIncrementWsConnection() {
        binder.incrementWsConnection();
        double count = registry.get("netty.websocket.connection").counter().count();
        assertEquals(1.0, count, 0.01);
    }

    @Test
    void testMultipleIncrements() {
        binder.incrementBusinessMsg();
        binder.incrementBusinessMsg();
        binder.incrementBusinessMsg();
        double count = registry.get("netty.business.message").counter().count();
        assertEquals(3.0, count, 0.01);
    }
}
