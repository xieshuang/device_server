package com.xsh.netty.handler;

import com.xsh.netty.server.IpFirewallService;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

/**
 * IP 过滤 Handler，在 Pipeline 最前端检查客户端 IP 是否在黑名单中。
 *
 * <p>位于 SslHandler 之后、ReadTimeoutHandler 之前，
 * 在连接建立（channelActive）时立即检查，命中黑名单则直接关闭连接。
 */
@Slf4j
public class IpFilterHandler extends ChannelInboundHandlerAdapter {

    private final IpFirewallService firewallService;

    public IpFilterHandler(IpFirewallService firewallService) {
        this.firewallService = firewallService;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        InetSocketAddress address = (InetSocketAddress) ctx.channel().remoteAddress();
        String remoteIp = address.getAddress().getHostAddress();

        if (firewallService.isBanned(remoteIp)) {
            log.warn("拒绝黑名单 IP 接入: {}", remoteIp);
            ctx.close();
            return;
        }

        ctx.fireChannelActive();
    }
}
