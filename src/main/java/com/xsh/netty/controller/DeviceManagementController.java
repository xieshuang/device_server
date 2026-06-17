package com.xsh.netty.controller;

import com.xsh.netty.auth.DeviceRevocationService;
import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.DeviceSession;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;

/**
 * 设备管理 REST API。
 */
@RestController
@RequestMapping("/api/devices")
public class DeviceManagementController {

    private final DeviceChannelManager channelManager;
    private final DeviceRevocationService revocationService;

    public DeviceManagementController(DeviceChannelManager channelManager,
                                       DeviceRevocationService revocationService) {
        this.channelManager = channelManager;
        this.revocationService = revocationService;
    }

    /** 在线设备列表 */
    @GetMapping("/online")
    public List<Map<String, Object>> getOnlineDevices() {
        List<Map<String, Object>> list = new ArrayList<>();
        for (Map.Entry<String, DeviceSession> entry : channelManager.getOnlineDevices().entrySet()) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("deviceId", entry.getKey());
            info.put("connectedAt", entry.getValue().getConnectTime());
            info.put("onlineDuration", Instant.now().toEpochMilli()
                    - entry.getValue().getConnectTime().toEpochMilli());
            list.add(info);
        }
        return list;
    }

    /** 设备详情 */
    @GetMapping("/{deviceId}")
    public Map<String, Object> getDevice(@PathVariable String deviceId) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("deviceId", deviceId);
        info.put("online", channelManager.getChannel(deviceId) != null);
        info.put("revoked", revocationService.isRevoked(deviceId));
        return info;
    }

    /** 强制下线 */
    @DeleteMapping("/{deviceId}")
    public Map<String, Object> forceOffline(@PathVariable String deviceId) {
        boolean success = channelManager.sendToDevice(deviceId, "FORCE_OFFLINE");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deviceId", deviceId);
        result.put("success", success);
        return result;
    }

    /** 吊销设备 */
    @PostMapping("/{deviceId}/revoke")
    public Map<String, Object> revoke(@PathVariable String deviceId,
                                       @RequestParam(defaultValue = "0") long ttlSeconds) {
        revocationService.revoke(deviceId, ttlSeconds);
        // 同时踢下线
        channelManager.sendToDevice(deviceId, "REVOKED");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deviceId", deviceId);
        result.put("revoked", true);
        result.put("ttlSeconds", ttlSeconds);
        return result;
    }

    /** 解除吊销 */
    @DeleteMapping("/{deviceId}/revoke")
    public Map<String, Object> unrevoke(@PathVariable String deviceId) {
        revocationService.unrevoke(deviceId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deviceId", deviceId);
        result.put("revoked", false);
        return result;
    }

    /** 统计信息 */
    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("onlineCount", channelManager.getOnlineCount());
        stats.put("totalDevices", channelManager.getOnlineDevices().size());
        return stats;
    }
}
