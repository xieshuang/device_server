package com.xsh.netty.auth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

/**
 * 设备吊销服务，基于 Redis 管理已吊销设备列表。
 *
 * <p>使用场景：
 * <ul>
 *   <li>安管发现设备异常行为 → 运维通过 API 立即吊销</li>
 *   <li>设备被盗/丢失 → 永久加入吊销列表</li>
 *   <li>设备固件检测到恶意代码 → 硬件端触发自吊销</li>
 * </ul>
 *
 * <p>Redis Key 设计：{@code device:revoked:{deviceId}}
 */
@Slf4j
@Service
public class DeviceRevocationService {

    private static final String REVOKED_PREFIX = "device:revoked:";

    private final StringRedisTemplate redisTemplate;

    public DeviceRevocationService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * 吊销设备（指定过期时长）。
     *
     * @param deviceId  设备ID
     * @param ttlSeconds 吊销时长（秒），≤0 表示永久
     */
    public void revoke(String deviceId, long ttlSeconds) {
        String key = REVOKED_PREFIX + deviceId;
        if (ttlSeconds > 0) {
            redisTemplate.opsForValue().set(key,
                    String.valueOf(System.currentTimeMillis()), ttlSeconds, TimeUnit.SECONDS);
            log.info("设备已临时吊销: deviceId={}, 时长={}秒", deviceId, ttlSeconds);
        } else {
            redisTemplate.opsForValue().set(key,
                    String.valueOf(System.currentTimeMillis()));
            log.info("设备已永久吊销: deviceId={}", deviceId);
        }
    }

    /**
     * 永久吊销设备。
     */
    public void revoke(String deviceId) {
        revoke(deviceId, 0);
    }

    /**
     * 解除设备吊销。
     */
    public void unrevoke(String deviceId) {
        redisTemplate.delete(REVOKED_PREFIX + deviceId);
        log.info("设备吊销已解除: deviceId={}", deviceId);
    }

    /**
     * 检查设备是否已吊销。
     *
     * @return true=已吊销，false=未吊销
     */
    public boolean isRevoked(String deviceId) {
        try {
            return Boolean.TRUE.equals(
                    redisTemplate.hasKey(REVOKED_PREFIX + deviceId));
        } catch (Exception e) {
            // Redis 不可用时 fail-open（放行），避免鉴权全部失败
            log.error("吊销状态查询异常，降级放行: deviceId={}", deviceId, e);
            return false;
        }
    }
}
