package com.xsh.netty.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 协议版本协商器单元测试。
 */
class VersionNegotiatorTest {

    @Test
    void testNegotiate_V2() {
        assertEquals(2, VersionNegotiator.negotiate((byte) 2));
    }

    @Test
    void testNegotiate_V1_Downgrade() {
        assertEquals(1, VersionNegotiator.negotiate((byte) 1));
    }

    @Test
    void testNegotiate_V3_DowngradeToV2() {
        // 客户端 V3，服务端最高 V2 → 降级到 V2
        assertEquals(2, VersionNegotiator.negotiate((byte) 3));
    }

    @Test
    void testNegotiate_V0_Fail() {
        // V0 低于最低兼容版本 V1 → 协商失败
        assertEquals(-1, VersionNegotiator.negotiate((byte) 0));
    }

    @Test
    void testNegotiate_NegativeVersion() {
        // 负数版本 → 低于 V1 → 失败
        assertEquals(-1, VersionNegotiator.negotiate((byte) -1));
    }
}
