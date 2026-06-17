package com.xsh.netty.controller;

import com.xsh.netty.ratelimit.RateLimiterService;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 限流管理 REST API。
 */
@RestController
@RequestMapping("/api/ratelimit")
public class RateLimitController {

    private final RateLimiterService rateLimiterService;

    public RateLimitController(RateLimiterService rateLimiterService) {
        this.rateLimiterService = rateLimiterService;
    }

    /** 动态调整全局速率 */
    @PutMapping("/global")
    public Map<String, Object> setGlobalRate(@RequestParam double permits) {
        rateLimiterService.setGlobalRate(permits);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("globalPermitsPerSecond", permits);
        return result;
    }

    /** 动态调整单设备速率 */
    @PutMapping("/device/{deviceId}")
    public Map<String, Object> setDeviceRate(@PathVariable String deviceId,
                                              @RequestParam double permits) {
        rateLimiterService.setDeviceRate(deviceId, permits);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("deviceId", deviceId);
        result.put("permitsPerSecond", permits);
        return result;
    }
}
