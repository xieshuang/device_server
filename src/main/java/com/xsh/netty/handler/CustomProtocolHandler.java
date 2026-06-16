package com.xsh.netty.handler;

import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.DeviceChannelManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;

/**
 * 自定义协议业务处理器，处理心跳检测和业务数据分发。
 *
 * <p>心跳检测机制：
 * <ul>
 *   <li>收到心跳请求（msgType=1）时，立即回复心跳响应（msgType=2，body="PONG"）</li>
 *   <li>任何数据到来都重置空闲计数器（说明连接存活）</li>
 *   <li>由 {@link io.netty.handler.timeout.IdleStateHandler} 触发读空闲事件，
 *       连续空闲达到 maxIdleCount 次后断开连接，避免网络瞬时抖动造成误杀</li>
 * </ul>
 *
 * <p>鉴权协同：
 * <ul>
 *   <li>此 Handler 位于 AuthHandler 之后，只有鉴权通过的消息才会到达此处</li>
 *   <li>可通过 {@link ChannelAttributes#DEVICE_ID} 获取设备ID</li>
 *   <li>AUTH_REQ/AUTH_RESP/AUTH_FAIL 由 AuthHandler 处理，不会到达此处</li>
 * </ul>
 *
 * <p>注意：此 Handler 由独立的 businessGroup 线程池执行，不阻塞 Netty I/O 线程。
 */
@Slf4j
public class CustomProtocolHandler extends SimpleChannelInboundHandler<MessagePacket> {

    /** 空闲计数器 AttributeKey，绑定在 Channel 上记录连续空闲次数 */
    private static final AttributeKey<Integer> IDLE_COUNTER = AttributeKey.valueOf("idleCounter");

    /** 最大允许连续空闲次数，超过此值将断开连接 */
    private final int maxIdleCount;
    /** 设备连接管理器 */
    private final DeviceChannelManager channelManager;

    public CustomProtocolHandler(int maxIdleCount, DeviceChannelManager channelManager) {
        this.maxIdleCount = maxIdleCount;
        this.channelManager = channelManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 连接建立时暂不注册，等鉴权成功后再注册
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 连接断开时注销设备
        String deviceId = ctx.channel().attr(ChannelAttributes.DEVICE_ID).get();
        if (deviceId != null) {
            channelManager.unregister(deviceId, ctx.channel());
        }
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) throws Exception {
        // 任何数据到来说明连接存活，清空空闲计数器
        ctx.channel().attr(IDLE_COUNTER).set(0);

        MessageHeader header = packet.getHeader();
        String deviceId = ctx.channel().attr(ChannelAttributes.DEVICE_ID).get();

        // 处理心跳请求：回复心跳响应
        if (header.getMsgType() == MsgType.HEARTBEAT_REQ) {
            log.debug("心跳请求: deviceId={}, 远程地址={}", deviceId, ctx.channel().remoteAddress());
            MessagePacket response = new MessagePacket();
            MessageHeader resHeader = new MessageHeader();
            resHeader.setMsgType(MsgType.HEARTBEAT_RESP);
            resHeader.setSerializationType(header.getSerializationType());
            response.setHeader(resHeader);
            response.setBody("PONG");
            ctx.writeAndFlush(response);
            return;
        }

        // 处理 ACK 确认消息
        if (header.getMsgType() == MsgType.ACK) {
            log.debug("收到ACK: deviceId={}, sequenceId={}", deviceId, header.getSequenceId());
            // ACK 由 PendingAckManager 处理，此处不再转发
            return;
        }

        // 处理业务数据：TODO 分发到具体业务逻辑（P1-2 MessageDispatcher）
        if (header.getMsgType() == MsgType.BUSINESS) {
            log.info("业务数据: deviceId={}, 数据={}", deviceId, packet.getBody());
        }
    }

    /**
     * 空闲事件回调，由 {@link io.netty.handler.timeout.IdleStateHandler} 触发。
     *
     * <p>连续空闲达到 maxIdleCount 次后断开连接，避免网络瞬时抖动造成误杀。
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            Integer counter = ctx.channel().attr(IDLE_COUNTER).get();
            if (counter == null) {
                counter = 0;
            }
            counter++;
            ctx.channel().attr(IDLE_COUNTER).set(counter);

            String deviceId = ctx.channel().attr(ChannelAttributes.DEVICE_ID).get();
            log.warn("空闲检测: deviceId={}, 计数={}", deviceId, counter);

            if (counter >= maxIdleCount) {
                log.error("连续空闲{}次，关闭连接: deviceId={}", maxIdleCount, deviceId);
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String deviceId = ctx.channel().attr(ChannelAttributes.DEVICE_ID).get();
        log.error("业务处理器异常: deviceId={}", deviceId, cause);
        ctx.close();
    }
}
