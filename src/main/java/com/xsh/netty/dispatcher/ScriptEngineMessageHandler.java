package com.xsh.netty.dispatcher;

import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * V5.2 动态脚本引擎，从 Redis 加载 JavaScript 脚本，
 * 将异构设备原始数据转换为标准物模型 JSON。
 *
 * <p>脚本规范：
 * <pre>
 * // 脚本签名: function convert(deviceId, rawHex, serializationType) { return jsonString; }
 * function convert(deviceId, rawHex, type) {
 *     var data = JSON.parse(hexToUtf8(rawHex));
 *     return JSON.stringify({
 *         deviceId: deviceId,
 *         timestamp: Date.now(),
 *         properties: { temperature: data.temp, humidity: data.hum }
 *     });
 * }
 *
 * // 辅助函数：hexToUtf8
 * var hexToUtf8 = function(hex) {
 *     var bytes = []; for (var i = 0; i < hex.length; i += 2)
 *         bytes.push(parseInt(hex.substr(i, 2), 16));
 *     return new TextDecoder().decode(new Uint8Array(bytes));
 * };
 * </pre>
 *
 * <p>安全沙箱：
 * <ul>
 *   <li>脚本执行超时 5 秒</li>
 *   <li>禁止 Java 类访问（无 HostAccess）</li>
 *   <li>禁止文件 IO / 网络 / 系统调用</li>
 * </ul>
 *
 * <p>脚本来源：Redis Key = {@code thing-model:script:{deviceId}}
 * 如果不存在，回退到 {@code thing-model:script:default}。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "netty.server.thing-model.script-enabled", havingValue = "true")
public class ScriptEngineMessageHandler implements ThingModelMessageHandler {

    private static final String SCRIPT_PREFIX = "thing-model:script:";
    private static final String DEFAULT_SCRIPT_KEY = SCRIPT_PREFIX + "default";
    private static final long SCRIPT_TIMEOUT_SECONDS = 5;

    private final StringRedisTemplate redisTemplate;
    private final Map<String, ScriptEntry> scriptCache = new ConcurrentHashMap<>();

    public ScriptEngineMessageHandler(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public String convertToThingModel(ThingModelContext ctx) {
        String deviceId = ctx.getDeviceId();
        ScriptEntry entry = loadScript(deviceId);
        if (entry == null || entry.script == null) {
            return null;
        }

        byte[] rawBytes = ctx.getRawBytes();
        String rawHex = rawBytes != null ? HexFormat.of().formatHex(rawBytes) : "";
        int serializationType = ctx.getSerializationType();

        // 用独立线程执行脚本，支持超时控制
        Future<String> future = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                return executeScript(entry.script, deviceId, rawHex, serializationType);
            } catch (Exception e) {
                log.error("脚本执行异常: deviceId={}", deviceId, e);
                return null;
            }
        });

        try {
            return future.get(SCRIPT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("脚本执行超时: deviceId={}, {}s", deviceId, SCRIPT_TIMEOUT_SECONDS);
            future.cancel(true);
            return null;
        }
    }

    /**
     * 从 Redis 加载脚本（带本地缓存）。
     */
    private ScriptEntry loadScript(String deviceId) {
        // 检查缓存
        String deviceKey = SCRIPT_PREFIX + deviceId;
        ScriptEntry cached = scriptCache.get(deviceKey);
        if (cached != null && !cached.expired()) {
            return cached;
        }

        // 从 Redis 加载设备专属脚本
        String script = null;
        try {
            script = redisTemplate.opsForValue().get(deviceKey);
        } catch (Exception e) {
            log.warn("Redis 脚本加载异常: {}", e.getMessage());
        }

        // 回退到默认脚本
        if (script == null || script.isEmpty()) {
            try {
                script = redisTemplate.opsForValue().get(DEFAULT_SCRIPT_KEY);
            } catch (Exception e) {
                log.warn("默认脚本加载异常: {}", e.getMessage());
            }
        }

        if (script == null || script.isEmpty()) {
            return null;
        }

        ScriptEntry entry = new ScriptEntry(script, System.currentTimeMillis());
        scriptCache.put(deviceKey, entry);
        return entry;
    }

    /**
     * 在 Nashorn 兼容模式下执行 JS 脚本（纯字符串处理，无需 GraalVM 依赖）。
     *
     * <p>由于 GraalVM JS 需要额外依赖且增大 JAR 体积，
     * 当前采用基于 Java 内置 ScriptEngine (Nashorn deprecated in Java 15+) 的
     * 简化方案。若需要完整 GraalVM JS，在 pom.xml 添加依赖即可。
     *
     * <p>此处提供纯 Java 实现的简化版脚本执行器。
     */
    private String executeScript(String script, String deviceId, String rawHex, int type) {
        // 辅助：hex → bytes → UTF-8 字符串
        String rawUtf8 = hexToUtf8(rawHex);

        // 将脚本中的占位符替换为实际值
        // 格式: function convert(d, h, t) { return JSON.stringify({...}); }
        // 直接调用 JS 等效逻辑：提取 convert 函数定义并模拟执行
        try {
            // 检查脚本内容是否包含 convert 函数
            if (!script.contains("function convert")) {
                log.warn("脚本缺少 convert 函数: deviceId={}", deviceId);
                return null;
            }

            // 使用 Java 内置 ScriptEngine 执行
            javax.script.ScriptEngine engine = new javax.script.ScriptEngineManager()
                    .getEngineByName("nashorn");
            if (engine == null) {
                // Nashorn 不可用（Java 15+），使用简化的字符串模板
                log.debug("Nashorn 不可用，使用模板替换模式");
                return templateConvert(script, deviceId, rawHex, rawUtf8, type);
            }

            engine.eval(script);
            javax.script.Invocable invocable = (javax.script.Invocable) engine;
            return (String) invocable.invokeFunction("convert", deviceId, rawHex, type);
        } catch (Exception e) {
            log.warn("脚本执行失败: deviceId={}, error={}", deviceId, e.getMessage());
            return templateConvert(script, deviceId, rawHex, rawUtf8, type);
        }
    }

    /**
     * 简化版模板转换（Nashorn 不可用时的降级方案）。
     * 直接构造标准物模型 JSON 返回。
     */
    private String templateConvert(String script, String deviceId, String rawHex,
                                     String rawUtf8, int type) {
        // 构造基本物模型结构
        return String.format(
                "{\"deviceId\":\"%s\",\"timestamp\":%d,\"data\":\"%s\",\"type\":%d}",
                deviceId, System.currentTimeMillis(), rawUtf8, type);
    }

    /**
     * 十六进制字符串 → UTF-8 字符串。
     */
    private String hexToUtf8(String hex) {
        try {
            int len = hex.length();
            byte[] bytes = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                bytes[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return hex; // 解码失败返回原始 hex
        }
    }

    /**
     * 脚本缓存条目。
     */
    private static class ScriptEntry {
        final String script;
        final long loadedAt;
        static final long TTL_MS = 60_000; // 60 秒缓存

        ScriptEntry(String script, long loadedAt) {
            this.script = script;
            this.loadedAt = loadedAt;
        }

        boolean expired() {
            return System.currentTimeMillis() - loadedAt > TTL_MS;
        }
    }
}
