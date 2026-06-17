package com.xsh.netty.handler;

import com.xsh.netty.auth.AuthService;
import com.xsh.netty.config.NettyMetricsBinder;
import com.xsh.netty.protocol.AuthRequest;
import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.protocol.VersionInfo;
import com.xsh.netty.protocol.VersionNegotiator;
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
    private final NettyMetricsBinder metricsBinder;

    public AuthHandler(AuthService authService, DeviceChannelManager channelManager,
                        NettyMetricsBinder metricsBinder) {
        this.authService = authService;
        this.channelManager = channelManager;
        this.metricsBinder = metricsBinder;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) throws Exception {
        byte msgType = packet.getHeader().getMsgType();

        // 允许 VERSION_NEGOTIATE 消息通过（认证前可协商版本）
        if (msgType == MsgType.VERSION_NEGOTIATE) {
            handleVersionNegotiate(ctx, packet);
            return;
        }

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
     * 处理协议版本协商请求。
     *
     * <p>客户端发送 VERSION_NEGOTIATE 消息声明支持版本，
     * 服务端取 min(clientVersion, SERVER_MAX_VERSION) 回复协商结果。
     * 协商后版本绑定到 Channel 属性，后续编解码按此版本执行。
     */
    private void handleVersionNegotiate(ChannelHandlerContext ctx, MessagePacket packet) {
        // 从 Body 中提取客户端请求的版本号
        byte clientVersion;
        Object body = packet.getBody();
        if (body instanceof byte[] bytes) {
            clientVersion = bytes.length > 0 ? bytes[0] : VersionInfo.SERVER_MAX_VERSION;
        } else if (body instanceof String s) {
            try {
                clientVersion = Byte.parseByte(s);
            } catch (NumberFormatException e) {
                clientVersion = VersionInfo.SERVER_MAX_VERSION;
            }
        } else {
            clientVersion = VersionInfo.SERVER_MAX_VERSION;
        }

        // 协商版本
        byte negotiated = VersionNegotiator.negotiate(clientVersion);
        if (negotiated < 0) {
            log.warn("协议版本协商失败: clientVersion={}, 关闭连接: {}",
                    clientVersion, ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // 将协商结果绑定到 Channel 属性
        ctx.channel().attr(ChannelAttributes.NEGOTIATED_VERSION).set(negotiated);

        // 回复协商结果
        MessagePacket resp = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.VERSION_NEGOTIATE);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        header.setVersion(negotiated);
        resp.setHeader(header);
        resp.setBody(String.valueOf(negotiated));
        ctx.writeAndFlush(resp);

        log.debug("协议版本协商完成: clientVersion={}, negotiated={}, 远程地址={}",
                clientVersion, negotiated, ctx.channel().remoteAddress());
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

            // 记录鉴权成功指标
            metricsBinder.incrementAuthSuccess();

            // 从 Pipeline 移除自身，后续消息不再经过鉴权检查
            ctx.pipeline().remove(this);
            log.info("设备鉴权通过，Pipeline 移除 AuthHandler: deviceId={}, channel={}",
                    deviceId, ctx.channel().id().asShortText());
        } else {
            // 记录鉴权失败指标
            metricsBinder.incrementAuthFail();

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
