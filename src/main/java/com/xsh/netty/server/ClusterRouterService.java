package com.xsh.netty.server;

import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.serialize.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.function.BiConsumer;

/**
 * 集群路由服务，通过 Redis Pub/Sub 实现跨节点指令转发。
 *
 * <p>工作原理：
 * <ul>
 *   <li>每个节点订阅自己的专属通道 cluster:command:{myNodeId}</li>
 *   <li>收到 Pub/Sub 消息后，解析 deviceId + message，查本地 Channel 转发</li>
 *   <li>routeCommand 向目标节点的专属通道发布指令</li>
 * </ul>
 */
@Slf4j
public class ClusterRouterService implements MessageListener {

    private static final String COMMAND_CHANNEL_PREFIX = "cluster:command:";

    private final StringRedisTemplate redisTemplate;
    private final String myNodeId;
    private final BiConsumer<String, Object> localDispatcher;

    /**
     * @param redisTemplate   Redis 模板
     * @param myNodeId        当前节点 ID
     * @param localDispatcher 本地消息分发器：(deviceId, message) → channel.writeAndFlush
     */
    public ClusterRouterService(StringRedisTemplate redisTemplate,
                                 String myNodeId,
                                 BiConsumer<String, Object> localDispatcher) {
        this.redisTemplate = redisTemplate;
        this.myNodeId = myNodeId;
        this.localDispatcher = localDispatcher;

        // 订阅本节点的指令通道
        String channel = COMMAND_CHANNEL_PREFIX + myNodeId;
        redisTemplate.getConnectionFactory().getConnection()
                .subscribe(this, channel.getBytes());
        log.info("集群路由服务已启动: nodeId={}, 订阅通道={}", myNodeId, channel);
    }

    /**
     * 将指令路由到目标节点（通过 Pub/Sub）。
     *
     * @param targetNodeId 目标节点 ID
     * @param deviceId     目标设备 ID
     * @param message      指令内容
     */
    public void routeCommand(String targetNodeId, String deviceId, Object message) {
        String channel = COMMAND_CHANNEL_PREFIX + targetNodeId;
        // 消息格式：deviceId\npayload
        String payload;
        if (message instanceof MessagePacket packet) {
            try {
                payload = new String(JsonSerializer.INSTANCE.serialize(packet));
            } catch (Exception e) {
                log.error("指令序列化失败: {}", e.getMessage());
                return;
            }
        } else {
            payload = message.toString();
        }
        redisTemplate.convertAndSend(channel, deviceId + "\n" + payload);
        log.debug("跨节点指令已路由: targetNode={}, deviceId={}", targetNodeId, deviceId);
    }

    /**
     * Redis Pub/Sub 消息回调：收到其他节点转发的指令。
     */
    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String msg = new String(message.getBody());
            int sep = msg.indexOf('\n');
            if (sep < 0) {
                log.warn("集群路由消息格式异常: {}", msg);
                return;
            }
            String deviceId = msg.substring(0, sep);
            String payload = msg.substring(sep + 1);

            localDispatcher.accept(deviceId, payload);
        } catch (Exception e) {
            log.error("集群路由消息处理异常", e);
        }
    }
}
