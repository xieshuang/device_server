package com.xsh.netty.server;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * IP 防火墙服务，管理动态黑白名单。
 *
 * <p>设计要点：
 * <ul>
 *   <li>本地 Caffeine 缓存记录 IP 失败计数（统计窗口内）</li>
 *   <li>超过阈值自动同步黑名单至 Redis，分配过期时间</li>
 *   <li>Redis 不可用时 fail-open（放行），不阻断正常业务</li>
 *   <li>黑名单到期后 Redis 自动删除，Cache 自动淘汰</li>
 * </ul>
 */
@Slf4j
@Service
public class IpFirewallService {

    private final StringRedisTemplate redisTemplate;
    private final NettyServerProperties properties;

    /** IP → 失败计数器（统计窗口自动过期） */
    private final Cache<String, LongAdder> failureCounter;
    /** 本地黑名单缓存，减少 Redis 查询 */
    private final Cache<String, Boolean> localBlacklist;

    public IpFirewallService(StringRedisTemplate redisTemplate, NettyServerProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;

        this.failureCounter = Caffeine.newBuilder()
                .expireAfterWrite(properties.getIpFilterBanWindowSeconds(), TimeUnit.SECONDS)
                .maximumSize(100_000)
                .build();

        this.localBlacklist = Caffeine.newBuilder()
                .expireAfterWrite(properties.getIpFilterBanDurationMinutes(), TimeUnit.MINUTES)
                .maximumSize(100_000)
                .removalListener((key, value, cause) -> {
                    if (cause == RemovalCause.EXPIRED) {
                        log.info("IP 封禁到期: {}", key);
                    }
                })
                .build();
    }

    /**
     * 检查 IP 是否在黑名单中。
     */
    public boolean isBanned(String ip) {
        // 先查本地缓存
        Boolean local = localBlacklist.getIfPresent(ip);
        if (local != null) return local;

        // 再查 Redis（fail-open：Redis 不可用时放行）
        try {
            Boolean redisBanned = redisTemplate.hasKey("ip:blacklist:" + ip);
            if (Boolean.TRUE.equals(redisBanned)) {
                localBlacklist.put(ip, true);
                return true;
            }
        } catch (Exception e) {
            log.warn("Redis 黑名单查询失败，放行 IP: {}", ip, e.getMessage());
        }
        return false;
    }

    /**
     * 记录一次协议失败（非法魔数、鉴权超时等），达到阈值时自动拉黑。
     */
    public void recordFailure(String ip) {
        LongAdder counter = failureCounter.get(ip, k -> new LongAdder());
        counter.increment();
        long count = counter.sum();

        if (count >= properties.getIpFilterBanThreshold()) {
            banIp(ip);
            failureCounter.invalidate(ip);
        }
    }

    /**
     * 封禁 IP：写入 Redis 黑名单 + 本地缓存。
     */
    private void banIp(String ip) {
        try {
            String key = "ip:blacklist:" + ip;
            redisTemplate.opsForValue().set(key, "banned",
                    properties.getIpFilterBanDurationMinutes(), TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("Redis 黑名单写入失败: ip={}", ip, e);
        }
        localBlacklist.put(ip, true);
        log.warn("IP 已被自动封禁: {}, 时长: {} 分钟",
                ip, properties.getIpFilterBanDurationMinutes());
    }
}
