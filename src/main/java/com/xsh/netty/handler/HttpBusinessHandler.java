package com.xsh.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpObject;
import lombok.extern.slf4j.Slf4j;

/**
 * HTTP 协议业务处理器。
 *
 * <p>由 {@link com.xsh.netty.codec.MultiProtocolDetector} 检测到 HTTP 协议后，
 * Pipeline 中会动态插入 HttpServerCodec + HttpObjectAggregator，最终由此 Handler 处理。
 *
 * <p>注意：此 Handler 由独立的 businessGroup 线程池执行，不阻塞 Netty I/O 线程。
 */
@Slf4j
public class HttpBusinessHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        // TODO: 实现 HTTP 业务逻辑（路由、分发、响应）
        log.debug("HTTP request received from {}", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in HTTP handler", cause);
        ctx.close();
    }
}
