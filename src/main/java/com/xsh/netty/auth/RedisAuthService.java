package com.xsh.netty.auth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.CompletableFuture;

/**
 * 基于 Redis 的鉴权服务实现。
 *
 * <p>Redis Key 设计：
 * <ul>
 *   <li>{@code device:auth:{deviceId}} → productSecret（设备密钥）</li>
 *   <li>{@code device:revoked:{deviceId}} → timestamp（设备吊销，V5 新增）</li>
 *   <li>{@code auth:nonce:{nonce}} → "1"（防重放，V5 新增）</li>
 * </ul>
 *
 * <p>鉴权算法：HMAC-MD5(deviceId + timestamp, productSecret)，使用 {@link HmacUtils} 统一计算。
 *
 * <p>V5 安全增强：nonce 防重放 + 设备吊销检查。
 */
@Slf4j
@Component
public class RedisAuthService implements AuthService {

    /** Redis Key 前缀 */
    private static final String KEY_PREFIX = "device:auth:";

    /** 时间戳允许偏差（毫秒），超过此范围视为重放攻击 */
    private static final long TIMESTAMP_TOLERANCE_MS = 5 * 60 * 1000;

    /** nonce TTL（秒），与 timestamp 容差窗口一致 */
    private static final long NONCE_TTL_SECONDS = 300;

    private final StringRedisTemplate redisTemplate;
    private final NonceValidator nonceValidator;
    private final DeviceRevocationService revocationService;

    public RedisAuthService(StringRedisTemplate redisTemplate,
                             NonceValidator nonceValidator,
                             DeviceRevocationService revocationService) {
        this.redisTemplate = redisTemplate;
        this.nonceValidator = nonceValidator;
        this.revocationService = revocationService;
    }

    @Override
    public CompletableFuture<Boolean> authenticate(String deviceId, long timestamp, String token) {
        // 兼容旧接口（无 nonce 参数），默认无 nonce
        return authenticate(deviceId, timestamp, token, null);
    }

    /**
     * V5 增强鉴权：增加 nonce 防重放 + 设备吊销检查。
     *
     * @param deviceId  设备ID
     * @param timestamp 客户端时间戳
     * @param token     HMAC 签名
     * @param nonce     防重放随机数（旧客户端可为 null）
     */
    public CompletableFuture<Boolean> authenticate(String deviceId, long timestamp,
                                                    String token, String nonce) {
        // 1. 校验时间戳偏差，防重放攻击
        long now = System.currentTimeMillis();
        if (Math.abs(now - timestamp) > TIMESTAMP_TOLERANCE_MS) {
            log.warn("鉴权失败，时间戳偏差过大: deviceId={}, timestamp={}, 当前={}, 偏差={}ms",
                    deviceId, timestamp, now, Math.abs(now - timestamp));
            return CompletableFuture.completedFuture(false);
        }

        // 2. V5：检查设备是否已吊销
        if (revocationService.isRevoked(deviceId)) {
            log.warn("鉴权失败，设备已吊销: deviceId={}", deviceId);
            return CompletableFuture.completedFuture(false);
        }

        // 3. V5：nonce 防重放校验
        if (nonce != null && !nonce.isEmpty()) {
            if (!nonceValidator.validateAndRecord(nonce, NONCE_TTL_SECONDS)) {
                log.warn("鉴权失败，nonce 重放攻击: deviceId={}", deviceId);
                return CompletableFuture.completedFuture(false);
            }
        }

        // 4. 异步从 Redis 获取 productSecret
        return CompletableFuture.supplyAsync(() -> {
            try {
                String productSecret = redisTemplate.opsForValue().get(KEY_PREFIX + deviceId);
                if (productSecret == null) {
                    log.warn("鉴权失败，设备未注册: deviceId={}", deviceId);
                    return false;
                }

                // 5. 使用 HmacUtils 计算 HMAC 并比对
                String expectedToken = HmacUtils.computeToken(deviceId, timestamp, productSecret);
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
     * 启动时校验 Redis 连接可用性。
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
