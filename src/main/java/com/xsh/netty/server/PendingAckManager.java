package com.xsh.netty.server;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.xsh.netty.protocol.MessagePacket;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 待确认消息管理器，基于 Caffeine TTL 本地缓存。
 *
 * <p>核心设计：
 * <ul>
 *   <li>发送需要确认的消息时，以 "channelId#sequenceId" 为 key 存入缓存</li>
 *   <li>收到 ACK 后根据 key 移除对应记录</li>
 *   <li>TTL 过期后自动触发回调：记录日志或执行重传逻辑</li>
 *   <li>Caffeine 自动淘汰过期条目，不会无限增长拖垮内存</li>
 * </ul>
 *
 * <p>为什么不用永久队列：
 * <ul>
 *   <li>工业物联网网络差时，确认等待队列可能无限增长</li>
 *   <li>Caffeine TTL 确保过期条目自动淘汰，内存占用可控</li>
 *   <li>过期回调用于日志告警，不自动重传（避免雪崩）</li>
 * </ul>
 */
@Slf4j
@Component
public class PendingAckManager {

    /** 默认 ACK 超时时间（秒） */
    private static final int DEFAULT_ACK_TIMEOUT_SECONDS = 30;

    /** 待确认消息缓存：key = channelId#sequenceId, value = MessagePacket */
    private final Cache<String, MessagePacket> pendingCache;

    public PendingAckManager() {
        this.pendingCache = Caffeine.newBuilder()
                .expireAfterWrite(DEFAULT_ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .maximumSize(100_000) // 最大10万条，防内存溢出
                .removalListener((key, value, cause) -> {
                    if (cause == com.github.benmanes.caffeine.cache.RemovalCause.EXPIRED) {
                        log.warn("消息确认超时: key={}, 消息将不重传", key);
                        // 工业场景下不自动重传，避免雪崩；仅告警
                    }
                })
                .build();
    }

    /**
     * 记录待确认消息。
     *
     * @param channelId  Channel 的短ID
     * @param sequenceId 消息序列号
     * @param packet     发送的消息包
     */
    public void pending(String channelId, int sequenceId, MessagePacket packet) {
        String key = buildKey(channelId, sequenceId);
        pendingCache.put(key, packet);
        log.debug("等待ACK确认: key={}", key);
    }

    /**
     * 收到 ACK 后移除待确认记录。
     *
     * @param channelId  Channel 的短ID
     * @param sequenceId 确认的消息序列号
     * @return true=成功移除（确认收到），false=未找到（可能已超时）
     */
    public boolean ack(String channelId, int sequenceId) {
        String key = buildKey(channelId, sequenceId);
        MessagePacket removed = pendingCache.getIfPresent(key);
        if (removed != null) {
            pendingCache.invalidate(key);
            log.debug("ACK确认成功: key={}", key);
            return true;
        }
        log.debug("ACK未找到对应记录（可能已超时）: key={}", key);
        return false;
    }

    /**
     * 获取当前待确认消息数量。
     */
    public long pendingCount() {
        return pendingCache.estimatedSize();
    }

    private String buildKey(String channelId, int sequenceId) {
        return channelId + "#" + sequenceId;
    }
}
