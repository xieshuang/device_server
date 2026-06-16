package com.xsh.netty.server;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

/**
 * 设备会话信息，记录设备连接的关键属性。
 *
 * <p>用于 DeviceChannelManager 中 deviceId → 会话的映射。
 */
@Data
@AllArgsConstructor
public class DeviceSession {

    /** 设备ID */
    private String deviceId;

    /** 设备连接的 Channel */
    private Channel channel;

    /** 连接建立时间 */
    private Instant connectTime;

    /**
     * 仅比较 deviceId 和 channel 引用，用于 ConcurrentHashMap.remove 的精确匹配。
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeviceSession that)) return false;
        return deviceId.equals(that.deviceId) && channel == that.channel;
    }

    @Override
    public int hashCode() {
        return 31 * deviceId.hashCode() + System.identityHashCode(channel);
    }
}
