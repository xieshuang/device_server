package com.xsh.netty.dispatcher;

import com.xsh.netty.protocol.MessagePacket;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消息分发器，根据 msgType 将消息路由到对应的 MessageHandler。
 *
 * <p>核心设计：
 * <ul>
 *   <li>通过 {@link #register(MessageHandler)} 注册处理器</li>
 *   <li>通过 {@link #dispatch(ChannelHandlerContext, String, MessagePacket)} 路由消息</li>
 *   <li>未找到对应 Handler 时走默认处理（打印日志）</li>
 *   <li>Spring Bean 自动注册：容器启动时自动收集所有 MessageHandler 实现</li>
 * </ul>
 */
@Slf4j
public class MessageDispatcher {

    /** msgType → MessageHandler 映射 */
    private final Map<Byte, MessageHandler> handlerMap = new ConcurrentHashMap<>();

    /**
     * 注册消息处理器。
     */
    public void register(MessageHandler handler) {
        handlerMap.put(handler.supportMsgType(), handler);
        log.info("消息处理器已注册: msgType={}, handler={}", handler.supportMsgType(), handler.getClass().getSimpleName());
    }

    /**
     * 批量注册消息处理器。
     */
    public void registerAll(List<MessageHandler> handlers) {
        handlers.forEach(this::register);
    }

    /**
     * 分发消息到对应的处理器。
     */
    public void dispatch(ChannelHandlerContext ctx, String deviceId, MessagePacket packet) {
        byte msgType = packet.getHeader().getMsgType();
        MessageHandler handler = handlerMap.get(msgType);

        if (handler != null) {
            handler.handle(ctx, deviceId, packet);
        } else {
            // 默认处理：打印日志
            log.warn("未注册的消息类型: msgType={}, deviceId={}, body={}",
                    msgType, deviceId, packet.getBody());
        }
    }
}
