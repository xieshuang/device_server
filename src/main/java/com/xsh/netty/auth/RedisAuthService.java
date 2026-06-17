package com.xsh.netty.auth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.CompletableFuture;

/**
 * 基于 Redis 的鉴权服务实现。
 *
 * <p>Redis Key 设计：{@code device:auth:{deviceId}} → productSecret
 *
 * <p>鉴权算法：HMAC-MD5(deviceId + timestamp, productSecret)，使用 {@link HmacUtils} 统一计算。
 *
 * <p>防重放攻击：timestamp 与服务端时间差超过 5 分钟视为无效请求。
 */
@Slf4j
@Component
public class RedisAuthService implements AuthService {

    /** Redis Key 前缀 */
    private static final String KEY_PREFIX = "device:auth:";

    /** 时间戳允许偏差（毫秒），超过此范围视为重放攻击 */
    private static final long TIMESTAMP_TOLERANCE_MS = 5 * 60 * 1000;

    private final StringRedisTemplate redisTemplate;

    public RedisAuthService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public CompletableFuture<Boolean> authenticate(String deviceId, long timestamp, String token) {
        // 1. 校验时间戳偏差，防重放攻击
        long now = System.currentTimeMillis();
        if (Math.abs(now - timestamp) > TIMESTAMP_TOLERANCE_MS) {
            log.warn("鉴权失败，时间戳偏差过大: deviceId={}, timestamp={}, 当前={}, 偏差={}ms",
                    deviceId, timestamp, now, Math.abs(now - timestamp));
            return CompletableFuture.completedFuture(false);
        }

        // 2. 异步从 Redis 获取 productSecret
        return CompletableFuture.supplyAsync(() -> {
            try {
                String productSecret = redisTemplate.opsForValue().get(KEY_PREFIX + deviceId);
                if (productSecret == null) {
                    log.warn("鉴权失败，设备未注册: deviceId={}", deviceId);
                    return false;
                }

                // 3. 使用 HmacUtils 计算 HMAC 并比对，确保与客户端算法一致
                String expectedToken = HmacUtils.computeToken(deviceId, timestamp, productSecret);
                // 注意：严禁在日志中打印 expectedToken，避免 token 泄露
                log.debug("server 端 HMAC 计算完成: deviceId={}", deviceId);
                boolean valid = expectedToken.equalsIgnoreCase(token);

                if (!valid) {
                    log.warn("鉴权失败，token 不匹配: deviceId={}", deviceId);
                } else {
                    log.info("鉴权成功: deviceId={}", deviceId);
                }
                return valid;
            } catch (Exception e) {
                log.error("鉴权异常: deviceId={}", deviceId, e);
                return false;
            }
        });
    }

    /**
     * 启动时校验 Redis 连接可用性，连接异常时打印告警日志但不阻塞启动。
     *
     * <p>设计：Redis 作为外部依赖，不可用时不应阻止应用启动（可能只是临时抖动）。
     * 鉴权失败时已有防御性处理，因此此处仅告警。
     */
    @PostConstruct
    void checkRedisConnectivity() {
        try {
            String pong = redisTemplate.getConnectionFactory()
                    .getConnection().ping();
            if ("PONG".equals(pong)) {
                log.info("Redis 连接校验通过");
            } else {
                log.warn("Redis PING 返回异常: {}", pong);
            }
        } catch (Exception e) {
            log.error("Redis 连接不可用，鉴权将全部失败: {}", e.getMessage());
        }
    }
}
