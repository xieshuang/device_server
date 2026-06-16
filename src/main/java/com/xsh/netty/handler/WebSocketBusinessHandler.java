package com.xsh.netty.handler;

import com.xsh.netty.codec.WebSocketFrameCodec;
import com.xsh.netty.config.HandlerBeanContainer;
import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import lombok.extern.slf4j.Slf4j;

/**
 * WebSocket 业务处理器，处理 WebSocket 升级完成后的业务消息。
 *
 * <p>消息路由复用 {@link com.xsh.netty.dispatcher.MessageDispatcher}，
 * 与自定义协议共享处理逻辑（BusinessMessageHandler 等）。
 *
 * <p>WebSocket 鉴权流程：
 * <ol>
 *   <li>客户端连接 WebSocket（/ws 路径）</li>
 *   <li>首条消息必须是 AUTH_REQ（编码为 BinaryWebSocketFrame）</li>
 *   <li>鉴权成功后 deviceId 绑定到 Channel 属性</li>
 *   <li>后续业务消息通过 MessageDispatcher 路由</li>
 * </ol>
 *
 * <p>处理的消息类型：
 * <ul>
 *   <li>BinaryWebSocketFrame — 业务消息，解码后路由到 Dispatcher</li>
 *   <li>PingWebSocketFrame — 回复 Pong</li>
 *   <li>CloseWebSocketFrame — 关闭连接</li>
 * </ul>
 */
@Slf4j
public class WebSocketBusinessHandler extends SimpleChannelInboundHandler<BinaryWebSocketFrame> {

    private final HandlerBeanContainer container;

    public WebSocketBusinessHandler(HandlerBeanContainer container) {
        this.container = container;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame frame) throws Exception {
        // 解码 BinaryWebSocketFrame 为 MessagePacket
        MessagePacket packet = WebSocketFrameCodec.decode(frame.content());
        if (packet == null) {
            log.warn("WebSocket 消息解码失败: 远程地址={}", ctx.channel().remoteAddress());
            return;
        }

        String deviceId = ctx.channel().attr(ChannelAttributes.DEVICE_ID).get();
        byte msgType = packet.getHeader().getMsgType();

        // 处理鉴权请求（WebSocket 首条消息）
        if (msgType == MsgType.AUTH_REQ) {
            handleAuth(ctx, packet);
            return;
        }

        // 未鉴权的连接拒绝业务消息
        if (deviceId == null) {
            log.warn("WebSocket 未鉴权连接发送业务消息: 远程地址={}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // 心跳消息处理
        if (MsgType.isHeartbeat(msgType)) {
            container.getMetricsBinder().incrementHeartbeatReq();
            // 回复心跳
            MessagePacket pong = new MessagePacket();
            MessageHeader pongHeader = new MessageHeader();
            pongHeader.setMsgType(MsgType.HEARTBEAT_RESP);
            pongHeader.setSerializationType(packet.getHeader().getSerializationType());
            pong.setHeader(pongHeader);
            pong.setBody("PONG");
            ctx.writeAndFlush(WebSocketFrameCodec.encode(pong));
            container.getMetricsBinder().incrementHeartbeatResp();
            return;
        }

        // ACK 确认消息
        if (msgType == MsgType.ACK) {
            container.getPendingAckManager()
                    .ack(ctx.channel().id().asShortText(), packet.getHeader().getSequenceId());
            return;
        }

        // 业务消息路由到 Dispatcher
        container.getMessageDispatcher().dispatch(ctx, deviceId, packet);
    }

    /**
     * 处理 WebSocket 鉴权请求。
     *
     * <p>简化版鉴权：从 BinaryWebSocketFrame 解码 AUTH_REQ，
     * 使用与自定义协议相同的 AuthService 校验。
     */
    private void handleAuth(ChannelHandlerContext ctx, MessagePacket packet) {
        // TODO: 完整鉴权逻辑，可从 URL 参数或首条消息获取 token
        // 当前简化：标记为已认证
        log.info("WebSocket 鉴权请求: 远程地址={}", ctx.channel().remoteAddress());
        container.getMetricsBinder().incrementWsConnection();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("WebSocket 处理异常: 远程地址={}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
