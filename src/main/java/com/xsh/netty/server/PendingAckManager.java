package com.xsh.netty.server;

import com.xsh.netty.protocol.MessagePacket;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * 待确认消息管理器，基于 Netty HashedWheelTimer 时间轮算法。
 *
 * <p>核心设计（V4 重构）：
 * <ul>
 *   <li>双 Map 弱关联设计：packetMap 存消息体，timeoutMap 存定时器句柄</li>
 *   <li>LongAdder 替代 size() 估算，提供精确 pending 计数</li>
 *   <li>轮盘槽位 2048，大幅衰减高并发下哈希冲突引发的链表长度</li>
 *   <li>超时任务在时间轮单线程中执行，将清理开销从 O(log N) 降至 O(1)</li>
 * </ul>
 *
 * <p>超时策略：仅打 warn 日志触发生产告警，不自动重传（避免雪崩）。
 */
@Slf4j
@Component
public class PendingAckManager {

    /** 默认 ACK 超时时间（毫秒） */
    private static final long DEFAULT_ACK_TIMEOUT_MS = 30000;

    /** 时间轮：tick 100ms，槽位 2048，单线程驱动 */
    private final HashedWheelTimer timer;

    /** key → MessagePacket 映射，存储待确认的原始消息 */
    private final Map<String, MessagePacket> packetMap;

    /** key → Timeout 句柄映射，用于 ACK 收到后取消超时任务 */
    private final Map<String, Timeout> timeoutMap;

    /** 精确待确认计数（LongAdder 高并发性能优于 AtomicLong） */
    private final LongAdder pendingCounter;

    /** ACK 超时时长（毫秒），允许外部配置注入 */
    private final long ackTimeoutMs;

    public PendingAckManager() {
        this(DEFAULT_ACK_TIMEOUT_MS);
    }

    public PendingAckManager(long ackTimeoutMs) {
        this.ackTimeoutMs = ackTimeoutMs;
        // 扩大轮盘槽位至 2048，大幅衰减哈希冲突引发的链表长度
        this.timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS, 2048);
        this.packetMap = new ConcurrentHashMap<>();
        this.timeoutMap = new ConcurrentHashMap<>();
        this.pendingCounter = new LongAdder();
    }

    /**
     * 记录待确认消息。
     *
     * @param channelId  Channel 的短ID
     * @param sequenceId 消息序列号
     * @param packet     发送的消息包
     */
    public void pending(String channelId, int sequenceId, MessagePacket packet) {
        String key = channelId + ":" + sequenceId;
        packetMap.put(key, packet);
        pendingCounter.increment();

        // 提交延迟超时检测任务到时间轮
        Timeout timeout = timer.newTimeout(t -> {
            // 超时未确认清理：双 Map 弱关联设计保证原子移出
            MessagePacket missed = packetMap.remove(key);
            timeoutMap.remove(key);
            if (missed != null) {
                pendingCounter.decrement();
                log.warn("工业数据 ACK 接收超时，触发生产告警。Key: {}", key);
            }
        }, ackTimeoutMs, TimeUnit.MILLISECONDS);

        timeoutMap.put(key, timeout);
    }

    /**
     * 收到 ACK 后移除待确认记录。
     *
     * @param channelId  Channel 的短ID
     * @param sequenceId 确认的消息序列号
     * @return true=成功确认，false=未找到（可能已超时）
     */
    public boolean ack(String channelId, int sequenceId) {
        String key = channelId + ":" + sequenceId;
        MessagePacket removed = packetMap.remove(key);
        if (removed != null) {
            pendingCounter.decrement();
            Timeout timeout = timeoutMap.remove(key);
            if (timeout != null) {
                // cancel 仅标记取消，显式移除协助 GC
                timeout.cancel();
            }
            return true;
        }
        return false;
    }

    /**
     * 获取当前待确认消息数量（精确值）。
     */
    public long pendingCount() {
        return pendingCounter.sum();
    }

    /**
     * 销毁时间轮，释放资源。
     */
    public void destroy() {
        timer.stop();
    }
}
