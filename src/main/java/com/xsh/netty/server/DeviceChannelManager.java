package com.xsh.netty.server;

import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备连接管理器，维护 deviceId 与 Channel 的映射关系。
 *
 * <p>核心功能：
 * <ul>
 *   <li>注册：设备鉴权成功后，将 deviceId → Channel 绑定</li>
 *   <li>注销：设备断线后自动清理映射</li>
 *   <li>踢旧保新：同一 deviceId 重复登录时，踢掉旧连接</li>
 *   <li>查找：按 deviceId 获取 Channel，用于定向推送</li>
 *   <li>广播：向所有在线设备发送消息</li>
 *   <li>统计：查询在线设备数量和列表</li>
 * </ul>
 *
 * <p>线程安全：基于 ConcurrentHashMap，支持高并发读写。
 */
@Slf4j
@Component
public class DeviceChannelManager {

    /** deviceId → DeviceSession 本地映射 */
    private final ConcurrentHashMap<String, DeviceSession> sessionMap = new ConcurrentHashMap<>();

    /**
     * 注册设备连接。
     *
     * <p>如果同一 deviceId 已有旧连接，则踢掉旧连接（踢旧保新策略），
     * 确保同一设备同一时刻只有一个活跃连接。
     *
     * @param deviceId 设备ID
     * @param channel  设备连接的 Channel
     */
    public void register(String deviceId, Channel channel) {
        DeviceSession newSession = new DeviceSession(deviceId, channel, Instant.now());

        DeviceSession oldSession = sessionMap.put(deviceId, newSession);
        if (oldSession != null) {
            Channel oldChannel = oldSession.getChannel();
            if (oldChannel.isActive()) {
                log.info("设备重复登录，踢掉旧连接: deviceId={}, 旧channel={}, 新channel={}",
                        deviceId, oldChannel.id().asShortText(), channel.id().asShortText());
                oldChannel.close();
            }
        } else {
            log.info("设备上线: deviceId={}, channel={}", deviceId, channel.id().asShortText());
        }
    }

    /**
     * 注销设备连接。
     *
     * <p>仅在当前 Channel 与注册的 Channel 一致时才移除，
     * 避免新连接被旧连接的断开事件误删。
     *
     * @param deviceId 设备ID
     * @param channel  断开的 Channel
     */
    public void unregister(String deviceId, Channel channel) {
        boolean removed = sessionMap.remove(deviceId, new DeviceSession(deviceId, channel, null));
        if (removed) {
            log.info("设备下线: deviceId={}, channel={}", deviceId, channel.id().asShortText());
        }
    }

    /**
     * 按 deviceId 查找 Channel。
     *
     * @param deviceId 设备ID
     * @return 对应的 Channel，不存在或已断开则返回 null
     */
    public Channel getChannel(String deviceId) {
        DeviceSession session = sessionMap.get(deviceId);
        return session != null && session.getChannel().isActive() ? session.getChannel() : null;
    }

    /**
     * 向指定设备发送消息。
     *
     * @param deviceId 设备ID
     * @param message  消息对象（MessagePacket）
     * @return true=发送成功，false=设备不在线
     */
    public boolean sendToDevice(String deviceId, Object message) {
        Channel channel = getChannel(deviceId);
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(message);
            return true;
        }
        return false;
    }

    /**
     * 获取当前在线设备数量。
     */
    public int getOnlineCount() {
        return sessionMap.size();
    }

    /**
     * 获取所有在线设备的 ID 集合（只读视图）。
     */
    public Map<String, DeviceSession> getOnlineDevices() {
        return Collections.unmodifiableMap(sessionMap);
    }

    /**
     * 向所有在线设备广播服务器维护通知。
     *
     * @param kafkaProducerService 用于异步投递离线遗言的 Kafka 服务（可为 null）
     */
    public void broadcastMaintenanceNotice() {
        MessagePacket notice = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSequenceId(0);
        notice.setHeader(header);
        notice.setBody("SERVER_MAINTENANCE: 网关正在维护，请稍后重连");

        for (DeviceSession session : sessionMap.values()) {
            Channel ch = session.getChannel();
            if (ch != null && ch.isActive()) {
                try {
                    ch.writeAndFlush(notice);
                } catch (Exception e) {
                    log.debug("维护通知发送失败: deviceId={}", session.getDeviceId());
                }
            }
        }
        log.info("维护通知已广播，覆盖 {} 台在线设备", sessionMap.size());
    }

    /**
     * 强制关闭所有在线设备连接。
     */
    public void closeAll() {
        for (DeviceSession session : sessionMap.values()) {
            Channel ch = session.getChannel();
            if (ch != null && ch.isActive()) {
                try {
                    ch.close();
                } catch (Exception e) {
                    log.debug("关闭连接异常: deviceId={}", session.getDeviceId());
                }
            }
        }
        sessionMap.clear();
        log.info("已关闭所有设备连接");
    }
}
