package com.xsh.netty.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * DeviceRevocationService 单元测试。
 */
class DeviceRevocationServiceTest {

    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private DeviceRevocationService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        service = new DeviceRevocationService(redisTemplate);
    }

    @Test
    void testRevoke_Temporary() {
        service.revoke("dev-001", 3600);
        verify(valueOps).set(eq("device:revoked:dev-001"), anyString(), eq(3600L), eq(TimeUnit.SECONDS));
    }

    @Test
    void testRevoke_Permanent() {
        service.revoke("dev-001"); // ttl=0 表示永久
        verify(valueOps).set(eq("device:revoked:dev-001"), anyString());
    }

    @Test
    void testUnrevoke() {
        service.unrevoke("dev-001");
        verify(redisTemplate).delete("device:revoked:dev-001");
    }

    @Test
    void testIsRevoked_True() {
        when(redisTemplate.hasKey("device:revoked:dev-001")).thenReturn(true);
        assertTrue(service.isRevoked("dev-001"));
    }

    @Test
    void testIsRevoked_False() {
        when(redisTemplate.hasKey("device:revoked:dev-001")).thenReturn(false);
        assertFalse(service.isRevoked("dev-001"));
    }

    @Test
    void testIsRevoked_RedisException_FailOpen() {
        when(redisTemplate.hasKey(anyString()))
                .thenThrow(new RuntimeException("Redis down"));
        // Redis 不可用时 fail-open（放行）
        assertFalse(service.isRevoked("dev-001"));
    }
}
