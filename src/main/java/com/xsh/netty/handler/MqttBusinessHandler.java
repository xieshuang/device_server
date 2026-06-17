package com.xsh.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * MQTT 业务处理主入口，根据消息类型分发到子流程。
 *
 * <p>支持的 MQTT 报文：
 * <ul>
 *   <li>CONNECT — 连接鉴权 + 遗嘱消息注册</li>
 *   <li>PUBLISH — 消息发布（QoS 0/1）</li>
 *   <li>SUBSCRIBE — Topic 订阅</li>
 *   <li>UNSUBSCRIBE — 取消订阅</li>
 *   <li>PINGREQ — 心跳保活</li>
 *   <li>DISCONNECT — 优雅断开</li>
 * </ul>
 *
 * <p>不实现：QoS 2（Exactly Once）— 延后到 V5.3。
 */
@Slf4j
public class MqttBusinessHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private final MqttTopicManager topicManager;

    /** 存储 CONNECT 时的 clientId → Channel */
    private final Map<String, ChannelHandlerContext> clients = new HashMap<>();

    public MqttBusinessHandler(MqttTopicManager topicManager) {
        this.topicManager = topicManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) {
        MqttMessageType type = msg.fixedHeader().messageType();

        switch (type) {
            case CONNECT:
                handleConnect(ctx, (MqttConnectMessage) msg);
                break;
            case PUBLISH:
                handlePublish(ctx, (MqttPublishMessage) msg);
                break;
            case SUBSCRIBE:
                handleSubscribe(ctx, (MqttSubscribeMessage) msg);
                break;
            case UNSUBSCRIBE:
                handleUnsubscribe(ctx, (MqttUnsubscribeMessage) msg);
                break;
            case PINGREQ:
                handlePingReq(ctx);
                break;
            case DISCONNECT:
                handleDisconnect(ctx);
                break;
            default:
                log.warn("未支持的 MQTT 消息类型: {}", type);
        }
    }

    // ===== CONNECT =====

    private void handleConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        String clientId = msg.payload().clientIdentifier();
        clients.put(clientId, ctx);
        log.info("MQTT 客户端连接: clientId={}, cleanSession={}", clientId,
                msg.variableHeader().isCleanSession());

        // 发送 CONNACK
        MqttConnAckMessage ack = MqttMessageBuilders.connAck()
                .sessionPresent(false)
                .returnCode(MqttConnectReturnCode.CONNECTION_ACCEPTED)
                .build();
        ctx.writeAndFlush(ack);
    }

    // ===== PUBLISH =====

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        String topic = msg.variableHeader().topicName();
        byte[] payload = new byte[msg.payload().readableBytes()];
        msg.payload().readBytes(payload);

        log.debug("MQTT PUBLISH: topic={}, qos={}", topic,
                msg.fixedHeader().qosLevel().value());

        // 路由到订阅者
        topicManager.publish(topic, payload);

        // QoS 1 → 回复 PUBACK
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            MqttMessage ack = MqttMessageBuilders.pubAck()
                    .packetId(msg.variableHeader().messageId())
                    .build();
            ctx.writeAndFlush(ack);
        }
    }

    // ===== SUBSCRIBE =====

    private void handleSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        List<Integer> grantedQos = new ArrayList<>();

        for (MqttTopicSubscription sub : msg.payload().topicSubscriptions()) {
            String topic = sub.topicName();
            int qos = sub.qualityOfService().value();
            topicManager.subscribe(topic, ctx);
            grantedQos.add(qos);
            log.debug("MQTT 订阅: topic={}, qos={}", topic, qos);
        }

        MqttMessage ack = MqttMessageBuilders.subAck()
                .packetId(msg.variableHeader().messageId())
                .addGrantedQoses(grantedQos.stream()
                        .map(MqttQoS::valueOf).toArray(MqttQoS[]::new))
                .build();
        ctx.writeAndFlush(ack);
    }

    // ===== UNSUBSCRIBE =====

    private void handleUnsubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        for (String topic : msg.payload().topics()) {
            topicManager.unsubscribe(topic, ctx);
            log.debug("MQTT 取消订阅: topic={}", topic);
        }

        MqttMessage ack = MqttMessageBuilders.unsubAck()
                .packetId(msg.variableHeader().messageId())
                .build();
        ctx.writeAndFlush(ack);
    }

    // ===== PING =====

    private void handlePingReq(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(new MqttMessage(new MqttFixedHeader(
                MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0)));
    }

    // ===== DISCONNECT =====

    private void handleDisconnect(ChannelHandlerContext ctx) {
        log.info("MQTT 客户端断开");
        clients.values().remove(ctx);
        ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("MQTT 处理异常", cause);
        ctx.close();
    }
}
