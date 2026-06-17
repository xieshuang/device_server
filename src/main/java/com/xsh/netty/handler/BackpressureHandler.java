package com.xsh.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * TCP 背压处理器，基于 channelWritabilityChanged 事件双向驱动。
 *
 * <p>当下游队列（Kafka、业务线程池）积压导致写缓冲区达到高水位时，
 * 通过关闭 autoRead 停止读取 TCP 缓冲区，利用 TCP 滑动窗口机制反向压制设备降频。
 *
 * <p>修正说明（V4）：原方案将背压逻辑写在 channelRead 内属于非幂等操作，
 * 极易产生连接单向卡死的安全事故。现修正为完全基于 channelWritabilityChanged
 * 状态变更事件进行纯双向驱动——可写恢复时重新开启自动读取。
 */
@Slf4j
public class BackpressureHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        boolean isWritable = ctx.channel().isWritable();
        if (isWritable) {
            // 恢复可写：重新打开自动读取，唤醒通信链路
            ctx.channel().config().setAutoRead(true);
            log.debug("设备连接通道 [{}] 恢复可写，重新启动自动读取", ctx.channel().remoteAddress());
        } else {
            // 高水位拦截：关闭自动读取，触发内核 TCP 反压机制
            ctx.channel().config().setAutoRead(false);
            log.warn("网关积压已达到高水位线！暂停通道 [{}] 自动读取以触发 TCP 反压",
                    ctx.channel().remoteAddress());
        }
        // 向后传播事件，确保后续 Handler 感知状态变化
        ctx.fireChannelWritabilityChanged();
    }
}
