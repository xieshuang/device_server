package com.xsh.netty.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * ClusterSessionManager 单元测试。
 */
class ClusterSessionManagerTest {

    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private ClusterSessionManager manager;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        manager = new ClusterSessionManager(redisTemplate, "node-1");
    }

    @Test
    void testRegisterSession() {
        manager.registerSession("dev-001");
        verify(valueOps).set("device:session:dev-001", "node-1");
    }

    @Test
    void testUnregisterSession_LuaDelete() {
        when(redisTemplate.execute(any(DefaultRedisScript.class),
                eq(Collections.singletonList("device:session:dev-001")), eq("node-1")))
                .thenReturn(1L);

        manager.unregisterSession("dev-001");
        // 调用了 Lua 脚本（带令牌校验）
        verify(redisTemplate).execute(any(DefaultRedisScript.class),
                eq(Collections.singletonList("device:session:dev-001")), eq("node-1"));
    }

    @Test
    void testUnregisterSession_WrongNode_NotDeleted() {
        // Lua 脚本返回 0（nodeId 不匹配，未删除）
        when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), eq("node-1")))
                .thenReturn(0L);

        // 不应抛异常
        assertDoesNotThrow(() -> manager.unregisterSession("dev-001"));
    }

    @Test
    void testGetNodeId_Found() {
        when(valueOps.get("device:session:dev-001")).thenReturn("node-2");
        assertEquals("node-2", manager.getNodeId("dev-001"));
    }

    @Test
    void testGetNodeId_NotFound() {
        when(valueOps.get("device:session:dev-001")).thenReturn(null);
        assertNull(manager.getNodeId("dev-001"));
    }
}
