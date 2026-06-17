package com.xsh.netty.auth;

import com.xsh.netty.protocol.*;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.PendingAckManager;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HasdedWheelTimer ACK 管理器单元测试。
 */
class PendingAckManagerTest {

    private final PendingAckManager ackManager = new PendingAckManager(1000); // 1s 超时

    @Test
    void testPendingAndAck_Success() {
        MessagePacket packet = new MessagePacket();
        packet.setHeader(new MessageHeader());

        ackManager.pending("ch-001", 1, packet);
        assertEquals(1, ackManager.pendingCount());

        boolean result = ackManager.ack("ch-001", 1);
        assertTrue(result);
        assertEquals(0, ackManager.pendingCount());
    }

    @Test
    void testAck_UnknownKey_ReturnsFalse() {
        boolean result = ackManager.ack("unknown", 999);
        assertFalse(result);
    }

    @Test
    void testAck_AlreadyAcked_ReturnsFalse() {
        MessagePacket packet = new MessagePacket();
        packet.setHeader(new MessageHeader());

        ackManager.pending("ch-002", 2, packet);
        ackManager.ack("ch-002", 2);
        boolean secondAck = ackManager.ack("ch-002", 2);
        assertFalse(secondAck);
    }

    @Test
    void testPendingCount_InitialZero() {
        assertEquals(0, ackManager.pendingCount());
    }

    @Test
    void testPendingMultipleKeys() {
        MessagePacket packet = new MessagePacket();
        packet.setHeader(new MessageHeader());

        ackManager.pending("ch-A", 1, packet);
        ackManager.pending("ch-A", 2, packet);
        ackManager.pending("ch-B", 1, packet);
        assertEquals(3, ackManager.pendingCount());

        ackManager.ack("ch-A", 1);
        assertEquals(2, ackManager.pendingCount());
    }

    @Test
    void testPendingThenTimeout() throws InterruptedException {
        MessagePacket packet = new MessagePacket();
        packet.setHeader(new MessageHeader());

        ackManager.pending("ch-timeout", 1, packet);
        assertEquals(1, ackManager.pendingCount());

        // 等待超时 (tick 100ms + 超时 1000ms，留一些余量)
        TimeUnit.MILLISECONDS.sleep(1500);

        // 超时后计数应归零
        assertEquals(0, ackManager.pendingCount());
        // 超时后 ack 应返回 false
        assertFalse(ackManager.ack("ch-timeout", 1));
    }
}
