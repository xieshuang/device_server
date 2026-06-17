package com.xsh.netty.controller;

import com.xsh.netty.server.IpFirewallService;
import com.xsh.netty.server.NettyServerProperties;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 防火墙管理 REST API。
 */
@RestController
@RequestMapping("/api/firewall")
public class FirewallController {

    private final IpFirewallService firewallService;
    private final NettyServerProperties properties;

    public FirewallController(IpFirewallService firewallService, NettyServerProperties properties) {
        this.firewallService = firewallService;
        this.properties = properties;
    }

    /** 查询 IP 是否被封禁 */
    @GetMapping("/banned/{ip}")
    public Map<String, Object> isBanned(@PathVariable String ip) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ip", ip);
        result.put("banned", firewallService.isBanned(ip));
        return result;
    }

    /** 手动封禁 IP */
    @PostMapping("/ban/{ip}")
    public Map<String, Object> banIp(@PathVariable String ip,
                                      @RequestParam(defaultValue = "60") long minutes) {
        // 调用 recordFailure 多次触发自动拉黑，或直接写入黑名单
        for (int i = 0; i < properties.getIpFilterBanThreshold(); i++) {
            firewallService.recordFailure(ip);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ip", ip);
        result.put("banned", true);
        result.put("durationMinutes", minutes);
        return result;
    }
}
