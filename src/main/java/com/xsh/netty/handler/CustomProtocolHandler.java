package com.xsh.netty.handler;

import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
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
 * <p>注意：此 Handler 由独立的 businessGroup 线程池执行，不阻塞 Netty I/O 线程。
 */
@Slf4j
public class CustomProtocolHandler extends SimpleChannelInboundHandler<MessagePacket> {

    /** 空闲计数器 AttributeKey，绑定在 Channel 上记录连续空闲次数 */
    private static final AttributeKey<Integer> IDLE_COUNTER = AttributeKey.valueOf("idleCounter");

    /** 最大允许连续空闲次数，超过此值将断开连接 */
    private final int maxIdleCount;

    public CustomProtocolHandler(int maxIdleCount) {
        this.maxIdleCount = maxIdleCount;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) throws Exception {
        // 任何数据到来说明连接存活，清空空闲计数器
        ctx.channel().attr(IDLE_COUNTER).set(0);

        MessageHeader header = packet.getHeader();

        // 处理心跳请求：回复心跳响应
        if (header.getMsgType() == MsgType.HEARTBEAT_REQ) {
            log.debug("Heartbeat from {}", ctx.channel().remoteAddress());
            MessagePacket response = new MessagePacket();
            MessageHeader resHeader = new MessageHeader();
            resHeader.setMsgType(MsgType.HEARTBEAT_RESP);
            resHeader.setSerializationType(header.getSerializationType());
            response.setHeader(resHeader);
            response.setBody("PONG");
            ctx.writeAndFlush(response);
            return;
        }

        // 处理业务数据：TODO 分发到具体业务逻辑
        if (header.getMsgType() == MsgType.BUSINESS) {
            log.info("Business data from {}: {}", ctx.channel().remoteAddress(), packet.getBody());
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

            log.warn("Channel idle detected: {}, count: {}", ctx.channel().remoteAddress(), counter);

            if (counter >= maxIdleCount) {
                log.error("Channel {} idle {} times, closing connection",
                        ctx.channel().remoteAddress(), maxIdleCount);
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception in custom protocol handler", cause);
        ctx.close();
    }
}
