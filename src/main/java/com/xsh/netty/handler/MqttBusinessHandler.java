package com.xsh.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * MQTT 协议业务处理器。
 *
 * <p>由 {@link com.xsh.netty.codec.MultiProtocolDetector} 检测到 MQTT 协议后，
 * Pipeline 中会动态插入 MqttDecoder + MqttEncoder，最终由此 Handler 处理。
 *
 * <p>注意：此 Handler 由独立的 businessGroup 线程池执行，不阻塞 Netty I/O 线程。
 */
@Slf4j
public class MqttBusinessHandler extends SimpleChannelInboundHandler<MqttMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        // TODO: 实现 MQTT 业务逻辑（订阅、发布、连接鉴权等）
        log.debug("MQTT message received from {}: type={}", ctx.channel().remoteAddress(),
                msg.fixedHeader() != null ? msg.fixedHeader().messageType() : "unknown");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in MQTT handler", cause);
        ctx.close();
    }
}
