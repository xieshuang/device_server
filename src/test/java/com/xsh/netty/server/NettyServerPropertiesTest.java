package com.xsh.netty.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NettyServerPropertiesTest {

    @Test
    void testDefaultValues() {
        NettyServerProperties props = new NettyServerProperties();
        assertEquals(9000, props.getPort());
        assertEquals(1, props.getBossThreads());
        assertEquals(0, props.getWorkerThreads());
        assertEquals(64, props.getBusinessThreads());
        assertEquals(5, props.getIdleTimeoutSeconds());
        assertEquals(3, props.getMaxIdleCount());
        assertEquals(10485760, props.getMaxFrameLength());
        assertEquals(1024, props.getSoBacklog());
    }
}
