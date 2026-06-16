package com.xsh.netty.ratelimit;

import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 限流 Handler，位于 Pipeline 中鉴权之后、业务处理之前。
 *
 * <p>只对已认证设备的非心跳消息进行限流。
 *
 * <p>Pipeline 位置：AuthHandler → [RateLimitHandler] → CustomProtocolHandler
 *
 * <p>限流策略：
 * <ul>
 *   <li>心跳消息（PING/PONG）不限流，避免误杀保活流量</li>
 *   <li>未认证消息不限流（由 AuthHandler 和 ReadTimeoutHandler 管理）</li>
 *   <li>限流后默认丢弃消息保持连接，可配置 close-on-limit=true 直接关闭</li>
 * </ul>
 */
@Slf4j
public class RateLimitHandler extends SimpleChannelInboundHandler<MessagePacket> {

    private final RateLimiterService rateLimiterService;
    private final NettyServerProperties properties;

    public RateLimitHandler(RateLimiterService rateLimiterService, NettyServerProperties properties) {
        this.rateLimiterService = rateLimiterService;
        this.properties = properties;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) throws Exception {
        // 心跳消息不限流，避免误杀保活流量
        if (MsgType.isHeartbeat(packet.getHeader().getMsgType())) {
            ctx.fireChannelRead(packet);
            return;
        }

        // 未认证消息不限流（由 AuthHandler 管理）
        String deviceId = ctx.channel().attr(ChannelAttributes.DEVICE_ID).get();
        if (deviceId == null) {
            ctx.fireChannelRead(packet);
            return;
        }

        // 限流检查（全局 + 单设备双维度）
        if (!rateLimiterService.tryAcquire(deviceId)) {
            if (properties.isRateLimitCloseOnLimit()) {
                log.warn("限流关闭连接: deviceId={}", deviceId);
                ctx.close();
            } else {
                log.warn("限流丢弃消息: deviceId={}, msgType={}",
                        deviceId, packet.getHeader().getMsgType());
            }
            return;
        }

        ctx.fireChannelRead(packet);
    }
}
