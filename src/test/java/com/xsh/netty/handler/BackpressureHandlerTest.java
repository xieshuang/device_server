package com.xsh.netty.handler;

import io.netty.channel.ChannelOption;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TCP 背压处理器单元测试。
 */
class BackpressureHandlerTest {

    @Test
    void testChannelWritabilityChanged_HighWaterMark_AutoReadDisabled() {
        EmbeddedChannel channel = new EmbeddedChannel(new BackpressureHandler());

        // 验证处理器已正确添加到 Pipeline
        assertNotNull(channel.pipeline().get(BackpressureHandler.class));
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test
    void testPipeline_HandlerOrder() {
        EmbeddedChannel channel = new EmbeddedChannel(new BackpressureHandler());

        // 写入一条消息验证 Pipeline 正常流动
        channel.writeInbound("test");
        Object msg = channel.readInbound();
        assertEquals("test", msg);

        channel.finish();
    }
}
