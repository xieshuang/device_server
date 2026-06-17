package com.xsh.netty.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HMAC 工具类单元测试。
 */
class HmacUtilsTest {

    private static final String DEVICE_ID = "test-device-001";
    private static final String SECRET = "my-product-secret";

    @Test
    void testComputeToken_Consistency() {
        // 相同输入应产生相同输出
        long timestamp = 1718500000000L;
        String token1 = HmacUtils.computeToken(DEVICE_ID, timestamp, SECRET);
        String token2 = HmacUtils.computeToken(DEVICE_ID, timestamp, SECRET);
        assertEquals(token1, token2);
    }

    @Test
    void testComputeToken_DifferentDevice_DifferentToken() {
        long timestamp = System.currentTimeMillis();
        String token1 = HmacUtils.computeToken("dev-A", timestamp, SECRET);
        String token2 = HmacUtils.computeToken("dev-B", timestamp, SECRET);
        assertNotEquals(token1, token2);
    }

    @Test
    void testComputeToken_DifferentTimestamp_DifferentToken() {
        String token1 = HmacUtils.computeToken(DEVICE_ID, 1000L, SECRET);
        String token2 = HmacUtils.computeToken(DEVICE_ID, 2000L, SECRET);
        assertNotEquals(token1, token2);
    }

    @Test
    void testComputeToken_DifferentSecret_DifferentToken() {
        long timestamp = System.currentTimeMillis();
        String token1 = HmacUtils.computeToken(DEVICE_ID, timestamp, "secret-A");
        String token2 = HmacUtils.computeToken(DEVICE_ID, timestamp, "secret-B");
        assertNotEquals(token1, token2);
    }

    @Test
    void testComputeHmac_Format() {
        String result = HmacUtils.computeHmac("hello", SECRET);
        // HMAC-MD5 输出 32 位十六进制
        assertEquals(32, result.length());
        assertTrue(result.matches("[0-9a-f]{32}"));
    }
}
