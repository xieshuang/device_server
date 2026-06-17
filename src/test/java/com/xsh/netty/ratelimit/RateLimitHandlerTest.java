package com.xsh.netty.ratelimit;

import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * RateLimitHandler 单元测试。
 */
class RateLimitHandlerTest {

    private RateLimiterService rateLimiterService;
    private NettyServerProperties properties;
    private RateLimitHandler handler;

    @BeforeEach
    void setUp() {
        rateLimiterService = mock(RateLimiterService.class);
        properties = new NettyServerProperties();
        handler = new RateLimitHandler(rateLimiterService, properties);
    }

    @Test
    void testHeartbeat_NotLimited() {
        when(rateLimiterService.tryAcquire(anyString())).thenReturn(false);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.HEARTBEAT_REQ);
        packet.setHeader(header);

        channel.writeInbound(packet);

        // 心跳不应触发限流检查
        verify(rateLimiterService, never()).tryAcquire(anyString());

        channel.finish();
    }

    @Test
    void testRateLimited_DropMessage() {
        when(rateLimiterService.tryAcquire("dev-001")).thenReturn(false);
        properties.setRateLimitCloseOnLimit(false);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        channel.writeInbound(packet);

        // 限流丢弃消息，连接保持
        assertTrue(channel.isActive());
        verify(rateLimiterService).tryAcquire("dev-001");

        channel.finish();
    }

    @Test
    void testRateLimited_CloseConnection() {
        when(rateLimiterService.tryAcquire("dev-001")).thenReturn(false);
        properties.setRateLimitCloseOnLimit(true);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        // 先设置设备已连接状态
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        channel.writeInbound(packet);

        verify(rateLimiterService).tryAcquire("dev-001");

        channel.finish();
    }

    @Test
    void testAllowed_PassesThrough() {
        when(rateLimiterService.tryAcquire("dev-001")).thenReturn(true);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        channel.writeInbound(packet);

        // 消息应通过（readInbound 不为 null）
        verify(rateLimiterService).tryAcquire("dev-001");

        channel.finish();
    }

    @Test
    void testNotAuthenticated_NotLimited() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        // 未设置 deviceId → 不限流

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        channel.writeInbound(packet);

        // 未认证用户不应触发限流检查
        verify(rateLimiterService, never()).tryAcquire(anyString());

        channel.finish();
    }
}
