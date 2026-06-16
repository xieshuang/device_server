package com.xsh.netty.dispatcher;

import com.xsh.netty.protocol.MessagePacket;
import io.netty.channel.ChannelHandlerContext;

/**
 * 业务消息处理器接口。
 *
 * <p>实现此接口并注册为 Spring Bean，即可自动接入消息分发体系。
 * 每个 Handler 声明自己处理的消息类型（msgType），Dispatcher 据此路由。
 */
public interface MessageHandler {

    /**
     * 此 Handler 处理的消息类型。
     */
    byte supportMsgType();

    /**
     * 处理业务消息。
     *
     * @param ctx    Channel 上下文
     * @param deviceId 设备ID（从 Channel 属性获取）
     * @param packet 消息包
     */
    void handle(ChannelHandlerContext ctx, String deviceId, MessagePacket packet);
}
