package com.xsh.netty.server;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;

/**
 * 分布式集群会话管理器，基于 Redis 存储设备-节点路由表。
 *
 * <p>核心设计（V4 修正）：
 * <ul>
 *   <li>使用 Lua 脚本原子令牌校验：unregister 时传入 myNodeId 作为校验令牌</li>
 *   <li>仅当 Redis 中存储的 nodeId 与当前节点一致时才执行删除</li>
 *   <li>彻底解决设备闪断重连到新节点后，旧节点延迟 channelInactive 误删合法路由的竞态问题</li>
 * </ul>
 */
@Slf4j
public class ClusterSessionManager {

    private static final String SESSION_KEY_PREFIX = "device:session:";

    /** 原子安全的 Lua 剔除脚本：仅当 nodeId 匹配时才执行删除 */
    private static final String UNREGISTER_LUA =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "   return redis.call('del', KEYS[1]) " +
            "else " +
            "   return 0 " +
            "end";

    private final DefaultRedisScript<Long> unregisterScript;

    private final StringRedisTemplate redisTemplate;
    private final String myNodeId;

    public ClusterSessionManager(StringRedisTemplate redisTemplate, String myNodeId) {
        this.redisTemplate = redisTemplate;
        this.myNodeId = myNodeId;
        this.unregisterScript = new DefaultRedisScript<>(UNREGISTER_LUA, Long.class);
    }

    /**
     * 注册设备会话到 Redis 路由表。
     */
    public void registerSession(String deviceId) {
        String redisKey = SESSION_KEY_PREFIX + deviceId;
        redisTemplate.opsForValue().set(redisKey, myNodeId);
        log.debug("集群会话注册: deviceId={}, nodeId={}", deviceId, myNodeId);
    }

    /**
     * 从 Redis 路由表注销设备会话（Lua 原子令牌校验）。
     */
    public void unregisterSession(String deviceId) {
        String redisKey = SESSION_KEY_PREFIX + deviceId;
        Long result = redisTemplate.execute(
                unregisterScript,
                Collections.singletonList(redisKey),
                myNodeId
        );
        if (result != null && result > 0) {
            log.debug("集群会话注销: deviceId={}, nodeId={}", deviceId, myNodeId);
        }
    }

    /**
     * 查询设备当前连接的节点 ID。
     *
     * @return 节点 ID，不存在则返回 null
     */
    public String getNodeId(String deviceId) {
        return redisTemplate.opsForValue().get(SESSION_KEY_PREFIX + deviceId);
    }
}
