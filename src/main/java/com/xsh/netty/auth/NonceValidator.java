package com.xsh.netty.auth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * Nonce 防重放校验器，基于 Redis SETNX 原子操作。
 *
 * <p>工作原理：
 * <ul>
 *   <li>客户端在 AUTH_REQ 中携带随机 nonce 字符串</li>
 *   <li>服务端通过 Redis SETNX 检查 nonce 是否已使用</li>
 *   <li>首次使用记录并返回 true，重复使用返回 false</li>
 *   <li>nonce 在 Redis 中 TTL = timestamp 容差窗口（5min），自动过期清理</li>
 * </ul>
 *
 * <p>兼容性：旧客户端不发送 nonce 时降级为仅 timestamp 校验。
 */
@Slf4j
@Service
public class NonceValidator {

    private static final String NONCE_PREFIX = "auth:nonce:";

    /** nonce 默认 TTL（秒），与 timestamp 容差窗口一致 */
    private static final long DEFAULT_NONCE_TTL_SECONDS = 300;

    private final StringRedisTemplate redisTemplate;

    public NonceValidator(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 校验 nonce 是否已使用。首次使用返回 true 并记录，重复使用返回 false。
     *
     * @param nonce     客户端生成的随机 nonce 字符串
     * @param ttlSeconds nonce 有效时长（秒），过期后 Redis 自动删除
     * @return true=首次使用（通过），false=已使用（重放攻击）
     */
    public boolean validateAndRecord(String nonce, long ttlSeconds) {
        if (nonce == null || nonce.isEmpty()) {
            // 无 nonce → 降级为仅 timestamp 校验
            return true;
        }

        try {
            String key = NONCE_PREFIX + nonce;
            Boolean success = redisTemplate.opsForValue()
                    .setIfAbsent(key, "1", ttlSeconds, TimeUnit.SECONDS);
            if (Boolean.TRUE.equals(success)) {
                return true;
            }
            log.warn("Nonce 重放攻击检测: nonce={}", nonce);
            return false;
        } catch (Exception e) {
            // Redis 不可用时 fail-open（放行），避免鉴权全部失败
            log.error("Nonce 校验异常，降级放行: {}", e.getMessage());
            return true;
        }
    }

    /**
     * 校验 nonce（使用默认 TTL = 5 分钟）。
     */
    public boolean validateAndRecord(String nonce) {
        return validateAndRecord(nonce, DEFAULT_NONCE_TTL_SECONDS);
    }
}
