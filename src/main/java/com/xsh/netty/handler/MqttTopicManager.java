package com.xsh.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * MQTT Topic 管理器，维护 Topic → Subscriber 映射。
 *
 * <p>支持通配符：
 * <ul>
 *   <li>单层通配符 +：匹配一层（如 home/+/temperature）</li>
 *   <li>多层通配符 #：匹配零层或多层（如 home/#）</li>
 * </ul>
 *
 * <p>发布时遍历所有订阅者，匹配 Topic 后转发。
 */
@Slf4j
public class MqttTopicManager {

    /** 全局单例 */
    public static final MqttTopicManager INSTANCE = new MqttTopicManager();

    /** Topic → 订阅者列表 */
    private final Map<String, Set<ChannelHandlerContext>> subscriptions = new ConcurrentHashMap<>();

    /**
     * 订阅 Topic。
     */
    public void subscribe(String topic, ChannelHandlerContext ctx) {
        subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet()).add(ctx);
    }

    /**
     * 取消订阅。
     */
    public void unsubscribe(String topic, ChannelHandlerContext ctx) {
        Set<ChannelHandlerContext> subs = subscriptions.get(topic);
        if (subs != null) {
            subs.remove(ctx);
        }
    }

    /**
     * 发布消息到匹配的订阅者。
     */
    public void publish(String topic, byte[] payload) {
        for (Map.Entry<String, Set<ChannelHandlerContext>> entry : subscriptions.entrySet()) {
            if (topicMatches(entry.getKey(), topic)) {
                for (ChannelHandlerContext ctx : entry.getValue()) {
                    if (ctx.channel().isActive()) {
                        ctx.channel().eventLoop().execute(() -> {
                            try {
                                // 构造 PUBLISH 并写入
                                io.netty.handler.codec.mqtt.MqttPublishMessage msg =
                                        io.netty.handler.codec.mqtt.MqttMessageBuilders.publish()
                                                .topicName(topic)
                                                .payload(io.netty.buffer.Unpooled.wrappedBuffer(payload))
                                                .qos(io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE)
                                                .build();
                                ctx.writeAndFlush(msg);
                            } catch (Exception e) {
                                log.error("MQTT 消息投递失败: topic={}", topic, e);
                            }
                        });
                    }
                }
            }
        }
    }

    /**
     * Topic 匹配（支持 + 和 # 通配符）。
     */
    static boolean topicMatches(String filter, String topic) {
        // 恰好相等
        if (filter.equals(topic)) return true;

        // 转换为正则：+ → [^/]+ ，# → .*
        String regex = filter
                .replace("/", "\\/")
                .replace("+", "[^/]+")
                .replace("#", ".*");
        return Pattern.matches("^" + regex + "$", topic);
    }
}
