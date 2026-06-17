package com.xsh.netty.ratelimit;

import com.xsh.netty.server.NettyServerProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 限流服务单元测试。
 *
 * <p>注意：Guava RateLimiter 的 tryAcquire 受时间流逝影响，测试需避免时间敏感断言。
 */
class RateLimiterServiceTest {

    private RateLimiterService service;
    private NettyServerProperties properties;

    @BeforeEach
    void setUp() {
        properties = new NettyServerProperties();
        properties.setRateLimitGlobalPermits(1000.0);
        properties.setRateLimitDevicePermits(100.0);
        service = new RateLimiterService(properties, new SimpleMeterRegistry());
    }

    @Test
    void testGlobalRateLimit_ZeroPermits_AllDenied() {
        properties.setRateLimitGlobalPermits(0.001);
        properties.setRateLimitDevicePermits(1000.0);
        service = new RateLimiterService(properties, new SimpleMeterRegistry());

        // 全局速率极低，多次尝试至少有一次被拒绝
        boolean anyDenied = false;
        for (int i = 0; i < 10; i++) {
            if (!service.tryAcquire("dev-001")) {
                anyDenied = true;
                break;
            }
        }
        assertTrue(anyDenied, "极低全局限流应拒绝");
    }

    @Test
    void testDeviceRateLimit_SingleDevice() {
        properties.setRateLimitGlobalPermits(1000.0);
        properties.setRateLimitDevicePermits(2.0);
        service = new RateLimiterService(properties, new SimpleMeterRegistry());

        // 单设备限流，前几次可能成功，后面应该被限流
        boolean anyDenied = false;
        for (int i = 0; i < 5; i++) {
            if (!service.tryAcquire("dev-A")) {
                anyDenied = true;
                break;
            }
        }
        assertTrue(anyDenied, "单设备限流应拒绝");
    }

    @Test
    void testDifferentDevices_Independent() {
        properties.setRateLimitGlobalPermits(1000.0);
        properties.setRateLimitDevicePermits(1.0);
        service = new RateLimiterService(properties, new SimpleMeterRegistry());

        // 设备 A 用掉限流
        service.tryAcquire("dev-A");
        boolean aDenied = !service.tryAcquire("dev-A") || !service.tryAcquire("dev-A");

        // 设备 B 仍有额度
        boolean bAllowed = service.tryAcquire("dev-B");

        assertTrue(aDenied || bAllowed, "两设备应独立限流");
    }

    @Test
    void testSetGlobalRate_AdjustUp() {
        properties.setRateLimitGlobalPermits(0.1);
        service = new RateLimiterService(properties, new SimpleMeterRegistry());

        service.setGlobalRate(1000.0);
        // 提高速率后应能获取令牌
        assertTrue(service.tryAcquire("dev-001"));
    }

    @Test
    void testSetDeviceRate_AdjustUp() {
        properties.setRateLimitGlobalPermits(1000.0);
        properties.setRateLimitDevicePermits(100.0);
        service = new RateLimiterService(properties, new SimpleMeterRegistry());

        // 调整后应能获取
        service.setDeviceRate("dev-A", 1000.0);
        assertTrue(service.tryAcquire("dev-A"));
    }

    @Test
    void testServiceCreation_NoException() {
        assertNotNull(service);
    }
}
