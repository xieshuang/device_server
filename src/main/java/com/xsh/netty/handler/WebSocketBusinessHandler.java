package com.xsh.netty.handler;

import com.xsh.netty.codec.WebSocketFrameCodec;
import com.xsh.netty.config.HandlerBeanContainer;
import com.xsh.netty.protocol.AuthRequest;
import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
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
 *   <li>服务端从 WebSocket 帧中解码 AuthRequest，调用 AuthService 异步校验</li>
 *   <li>鉴权成功后 deviceId 绑定到 Channel 属性，调用 channelManager.register()</li>
 *   <li>后续业务消息通过 MessageDispatcher 路由</li>
 * </ol>
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
        if (deviceId == null
                || !Boolean.TRUE.equals(ctx.channel().attr(ChannelAttributes.AUTHENTICATED).get())) {
            log.warn("WebSocket 未鉴权连接发送业务消息: 远程地址={}, msgType={}",
                    ctx.channel().remoteAddress(), msgType);
            sendWsAuthFail(ctx, "请先发送鉴权请求");
            ctx.close();
            return;
        }

        // 心跳消息处理
        if (MsgType.isHeartbeat(msgType)) {
            container.getMetricsBinder().incrementHeartbeatReq();
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
            boolean confirmed = container.getPendingAckManager()
                    .ack(ctx.channel().id().asShortText(), packet.getHeader().getSequenceId());
            if (!confirmed) {
                log.debug("WebSocket ACK未找到对应消息: seqId={}", packet.getHeader().getSequenceId());
            }
            return;
        }

        // 业务消息路由到 Dispatcher
        container.getMessageDispatcher().dispatch(ctx, deviceId, packet);
    }

    /**
     * 处理 WebSocket 鉴权请求，复用与自定义协议相同的 AuthService 校验。
     *
     * <p>从 BinaryWebSocketFrame 中提取 AuthRequest 的 body 字节，
     * 反序列化后调用 AuthService.authenticate() 进行 HMAC 校验。
     */
    private void handleAuth(ChannelHandlerContext ctx, MessagePacket packet) {
        // 提取 AUTH_REQ Body 字节
        byte[] bodyBytes;
        if (packet.getRawBody() != null) {
            bodyBytes = packet.getRawBody();
        } else if (packet.getBody() instanceof byte[] bytes) {
            bodyBytes = bytes;
        } else if (packet.getBody() instanceof String s) {
            bodyBytes = s.getBytes();
        } else {
            log.warn("WebSocket 鉴权请求 Body 类型异常: {}", packet.getBody() != null
                    ? packet.getBody().getClass().getName() : "null");
            sendWsAuthFail(ctx, "鉴权请求格式错误");
            ctx.close();
            return;
        }

        // 反序列化 AuthRequest
        AuthRequest authReq;
        try {
            authReq = Serializer.getSerializer(packet.getHeader().getSerializationType())
                    .deserialize(AuthRequest.class, bodyBytes);
        } catch (Exception e) {
            log.warn("WebSocket 鉴权请求反序列化失败: {}", e.getMessage());
            sendWsAuthFail(ctx, "鉴权请求格式错误");
            ctx.close();
            return;
        }

        // 异步鉴权
        container.getAuthService()
                .authenticate(authReq.getDeviceId(), authReq.getTimestamp(),
                        authReq.getToken(), authReq.getNonce())
                .thenAccept(success -> {
                    // 确保在 EventLoop 线程中执行 Channel 操作
                    if (ctx.channel().eventLoop().inEventLoop()) {
                        handleWsAuthResult(ctx, authReq.getDeviceId(), success);
                    } else {
                        ctx.channel().eventLoop().execute(() ->
                                handleWsAuthResult(ctx, authReq.getDeviceId(), success));
                    }
                });
    }

    /**
     * 处理 WebSocket 鉴权结果。
     */
    private void handleWsAuthResult(ChannelHandlerContext ctx, String deviceId, boolean success) {
        if (!ctx.channel().isActive()) return;

        if (success) {
            // 绑定 deviceId 到 Channel 属性
            ctx.channel().attr(ChannelAttributes.DEVICE_ID).set(deviceId);
            ctx.channel().attr(ChannelAttributes.AUTHENTICATED).set(true);

            // 注册设备到连接管理器（踢旧保新）
            container.getChannelManager().register(deviceId, ctx.channel());

            // 回复鉴权成功
            sendWsAuthResp(ctx, deviceId);

            // 记录指标
            container.getMetricsBinder().incrementAuthSuccess();
            container.getMetricsBinder().incrementWsConnection();

            log.info("WebSocket 设备鉴权通过: deviceId={}, 远程地址={}",
                    deviceId, ctx.channel().remoteAddress());
        } else {
            container.getMetricsBinder().incrementAuthFail();
            sendWsAuthFail(ctx, "鉴权失败，token 不匹配或设备未注册");
            ctx.close();
        }
    }

    /**
     * 发送 WebSocket 鉴权成功响应。
     */
    private void sendWsAuthResp(ChannelHandlerContext ctx, String deviceId) {
        MessagePacket resp = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_RESP);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        resp.setHeader(header);
        resp.setBody(deviceId);
        ctx.writeAndFlush(WebSocketFrameCodec.encode(resp));
    }

    /**
     * 发送 WebSocket 鉴权失败响应。
     */
    private void sendWsAuthFail(ChannelHandlerContext ctx, String reason) {
        MessagePacket resp = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_FAIL);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        resp.setHeader(header);
        resp.setBody(reason);
        ctx.writeAndFlush(WebSocketFrameCodec.encode(resp));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("WebSocket 处理异常: 远程地址={}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
