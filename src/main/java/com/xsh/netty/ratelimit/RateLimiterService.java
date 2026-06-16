package com.xsh.netty.ratelimit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.RateLimiter;
import com.xsh.netty.server.NettyServerProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 限流服务，管理全局和单设备两级令牌桶。
 *
 * <p>设计要点：
 * <ul>
 *   <li>全局令牌桶：单一 RateLimiter，保护服务整体吞吐</li>
 *   <li>单设备令牌桶：基于 Caffeine 缓存，deviceId → RateLimiter，自动淘汰不活跃设备</li>
 *   <li>两级检查：先全局再单设备，任一级限流即拒绝</li>
 * </ul>
 *
 * <p>线程安全：Guava RateLimiter 是线程安全的；Caffeine 缓存也是线程安全的。
 * 使用 computeIfAbsent 保证同一 deviceId 的 RateLimiter 只创建一次。
 */
@Slf4j
@Service
public class RateLimiterService {

    /** 全局令牌桶，保护服务端整体吞吐 */
    private final RateLimiter globalLimiter;

    /** 单设备令牌桶缓存：deviceId → RateLimiter，10分钟未访问自动淘汰 */
    private final Cache<String, RateLimiter> deviceLimiters;

    /** 限流计数器（Micrometer） */
    private final Counter rateLimitCounter;

    /** 服务端配置 */
    private final NettyServerProperties properties;

    public RateLimiterService(NettyServerProperties properties, MeterRegistry registry) {
        this.properties = properties;
        this.globalLimiter = RateLimiter.create(properties.getRateLimitGlobalPermits());
        this.deviceLimiters = Caffeine.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .maximumSize(100_000)
                .build();
        this.rateLimitCounter = Counter.builder("netty.rate.limited")
                .description("被限流的消息计数")
                .register(registry);
    }

    /**
     * 尝试获取令牌（两级检查）。
     *
     * @param deviceId 设备ID，用于单设备维度限流
     * @return true=允许通过，false=被限流
     */
    public boolean tryAcquire(String deviceId) {
        // 1. 全局限流检查
        if (!globalLimiter.tryAcquire()) {
            log.warn("全局限流: deviceId={}", deviceId);
            rateLimitCounter.increment();
            return false;
        }

        // 2. 单设备限流检查
        RateLimiter deviceLimiter = deviceLimiters.get(deviceId,
                id -> RateLimiter.create(properties.getRateLimitDevicePermits()));
        if (!deviceLimiter.tryAcquire()) {
            log.warn("设备限流: deviceId={}", deviceId);
            rateLimitCounter.increment();
            return false;
        }

        return true;
    }

    /**
     * 动态调整全局速率（管理接口调用）。
     *
     * @param permitsPerSecond 新的全局每秒令牌数
     */
    public void setGlobalRate(double permitsPerSecond) {
        globalLimiter.setRate(permitsPerSecond);
        log.info("全局限流速率调整: {} permits/s", permitsPerSecond);
    }

    /**
     * 动态调整单设备速率。
     *
     * @param deviceId 设备ID
     * @param permitsPerSecond 新的每秒令牌数
     */
    public void setDeviceRate(String deviceId, double permitsPerSecond) {
        deviceLimiters.put(deviceId, RateLimiter.create(permitsPerSecond));
        log.info("设备限流速率调整: deviceId={}, {} permits/s", deviceId, permitsPerSecond);
    }
}
