package com.xsh.netty.auth;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * NonceValidator 单元测试。
 */
class NonceValidatorTest {

    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOps;
    private NonceValidator validator;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        validator = new NonceValidator(redisTemplate);
    }

    @Test
    void testValidateAndRecord_FirstUse_ReturnsTrue() {
        when(valueOps.setIfAbsent(anyString(), eq("1"), anyLong(), eq(TimeUnit.SECONDS)))
                .thenReturn(true);

        assertTrue(validator.validateAndRecord("nonce-12345"));
    }

    @Test
    void testValidateAndRecord_Duplicate_ReturnsFalse() {
        when(valueOps.setIfAbsent(anyString(), eq("1"), anyLong(), eq(TimeUnit.SECONDS)))
                .thenReturn(false);

        assertFalse(validator.validateAndRecord("nonce-12345"));
    }

    @Test
    void testValidateAndRecord_NullNonce_ReturnsTrue() {
        // 无 nonce 降级为仅 timestamp 校验
        assertTrue(validator.validateAndRecord(null));
    }

    @Test
    void testValidateAndRecord_EmptyNonce_ReturnsTrue() {
        assertTrue(validator.validateAndRecord(""));
    }

    @Test
    void testValidateAndRecord_RedisException_FailOpen() {
        when(valueOps.setIfAbsent(anyString(), any(), anyLong(), any()))
                .thenThrow(new RuntimeException("Redis down"));

        // Redis 不可用时 fail-open（放行）
        assertTrue(validator.validateAndRecord("nonce-12345"));
    }
}
