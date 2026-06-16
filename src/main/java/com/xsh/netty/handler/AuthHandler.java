package com.xsh.netty.handler;

import com.xsh.netty.auth.AuthService;
import com.xsh.netty.protocol.AuthRequest;
import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.DeviceChannelManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 鉴权 Handler，位于 Pipeline 中 Decoder 之后、业务 Handler 之前。
 *
 * <p>工作流程：
 * <ol>
 *   <li>未认证的连接只允许 AUTH_REQ 消息通过，其他消息类型一律拒绝</li>
 *   <li>收到 AUTH_REQ 后，从 Body 反序列化 AuthRequest，调用 AuthService 异步校验</li>
 *   <li>鉴权成功：将 deviceId 绑定到 Channel 属性，标记为已认证，从 Pipeline 移除自身</li>
 *   <li>鉴权失败：回复 AUTH_FAIL 并关闭连接</li>
 * </ol>
 *
 * <p>鉴权成功后此 Handler 从 Pipeline 移除，后续消息直接由 CustomProtocolHandler 处理，
 * 不再有鉴权检查的性能开销。
 */
@Slf4j
public class AuthHandler extends SimpleChannelInboundHandler<MessagePacket> {

    private final AuthService authService;
    private final DeviceChannelManager channelManager;

    public AuthHandler(AuthService authService, DeviceChannelManager channelManager) {
        this.authService = authService;
        this.channelManager = channelManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) throws Exception {
        byte msgType = packet.getHeader().getMsgType();

        // 只允许 AUTH_REQ 通过
        if (msgType != MsgType.AUTH_REQ) {
            log.warn("未认证连接发送非鉴权消息, msgType={}, 远程地址={}, 关闭连接",
                    msgType, ctx.channel().remoteAddress());
            sendAuthFail(ctx, "未认证，请先发送鉴权请求");
            ctx.close();
            return;
        }

        // 解析 AUTH_REQ Body
        AuthRequest authReq;
        try {
            byte[] bodyBytes;
            if (packet.getBody() instanceof byte[] bytes) {
                bodyBytes = bytes;
            } else {
                bodyBytes = Serializer.getSerializer(packet.getHeader().getSerializationType())
                        .serialize(packet.getBody());
            }
            authReq = Serializer.getSerializer(packet.getHeader().getSerializationType())
                    .deserialize(AuthRequest.class, bodyBytes);
        } catch (Exception e) {
            log.warn("鉴权请求解析失败: {}", e.getMessage());
            sendAuthFail(ctx, "鉴权请求格式错误");
            ctx.close();
            return;
        }

        // 异步校验
        authService.authenticate(authReq.getDeviceId(), authReq.getTimestamp(), authReq.getToken())
                .thenAccept(success -> {
                    // 确保在 EventLoop 线程中执行 Pipeline 操作
                    if (ctx.channel().eventLoop().inEventLoop()) {
                        handleAuthResult(ctx, authReq.getDeviceId(), success);
                    } else {
                        ctx.channel().eventLoop().execute(() ->
                                handleAuthResult(ctx, authReq.getDeviceId(), success));
                    }
                });
    }

    /**
     * 处理鉴权结果。
     */
    private void handleAuthResult(ChannelHandlerContext ctx, String deviceId, boolean success) {
        if (!ctx.channel().isActive()) return;

        if (success) {
            // 绑定 deviceId 到 Channel 属性
            ctx.channel().attr(ChannelAttributes.DEVICE_ID).set(deviceId);
            ctx.channel().attr(ChannelAttributes.AUTHENTICATED).set(true);

            // 注册设备到连接管理器（踢旧保新）
            channelManager.register(deviceId, ctx.channel());

            // 回复鉴权成功
            sendAuthResp(ctx, deviceId);

            // 从 Pipeline 移除自身，后续消息不再经过鉴权检查
            ctx.pipeline().remove(this);
            log.info("设备鉴权通过，Pipeline 移除 AuthHandler: deviceId={}, channel={}",
                    deviceId, ctx.channel().id().asShortText());
        } else {
            sendAuthFail(ctx, "鉴权失败，token 不匹配或设备未注册");
            ctx.close();
        }
    }

    /**
     * 发送鉴权成功响应。
     */
    private void sendAuthResp(ChannelHandlerContext ctx, String deviceId) {
        MessagePacket resp = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_RESP);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        resp.setHeader(header);
        resp.setBody(deviceId);
        ctx.writeAndFlush(resp);
    }

    /**
     * 发送鉴权失败响应。
     */
    private void sendAuthFail(ChannelHandlerContext ctx, String reason) {
        MessagePacket resp = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_FAIL);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        resp.setHeader(header);
        resp.setBody(reason);
        ctx.writeAndFlush(resp);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("AuthHandler 异常", cause);
        ctx.close();
    }
}
