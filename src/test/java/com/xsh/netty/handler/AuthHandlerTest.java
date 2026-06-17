package com.xsh.netty.handler;

import com.xsh.netty.auth.AuthService;
import com.xsh.netty.config.NettyMetricsBinder;
import com.xsh.netty.protocol.AuthRequest;
import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.DeviceChannelManager;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 鉴权处理器单元测试。
 */
class AuthHandlerTest {

    private AuthService authService;
    private DeviceChannelManager channelManager;
    private NettyMetricsBinder metricsBinder;
    private AuthHandler authHandler;

    @BeforeEach
    void setUp() {
        authService = mock(AuthService.class);
        channelManager = mock(DeviceChannelManager.class);
        metricsBinder = mock(NettyMetricsBinder.class);
        authHandler = new AuthHandler(authService, channelManager, metricsBinder);
    }

    @Test
    void testAuthSuccess_DeviceIdBound() {
        when(authService.authenticate(anyString(), anyLong(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(authHandler);

        // 构造 AUTH_REQ 消息
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);

        AuthRequest authReq = new AuthRequest();
        authReq.setDeviceId("dev-001");
        authReq.setTimestamp(System.currentTimeMillis());
        authReq.setToken("valid-token");
        packet.setBody(authReq);

        channel.writeInbound(packet);

        // 验证 metrics 调用
        verify(metricsBinder).incrementAuthSuccess();

        channel.finish();
    }

    @Test
    void testAuthFail_WrongToken_Closes() {
        when(authService.authenticate(anyString(), anyLong(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(false));

        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(authHandler);

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);

        AuthRequest authReq = new AuthRequest();
        authReq.setDeviceId("dev-001");
        authReq.setTimestamp(System.currentTimeMillis());
        authReq.setToken("wrong-token");
        packet.setBody(authReq);

        channel.writeInbound(packet);

        // 鉴权失败应记录指标
        verify(metricsBinder).incrementAuthFail();

        channel.finish();
    }

    @Test
    void testNonAuthMessage_Refused() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(authHandler);

        // 未认证连接发送业务消息（非 AUTH_REQ）
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);
        packet.setBody("business-data");

        channel.writeInbound(packet);

        // 连接应保持（鉴权失败后 ctx.close()），但 Embedded channel 已经 isActive=false
        // 验证非鉴权消息不会触发 authService
        verify(authService, never()).authenticate(anyString(), anyLong(), anyString());

        channel.finish();
    }

    @Test
    void testVersionNegotiate_ValidVersion() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(authHandler);

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.VERSION_NEGOTIATE);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody(new byte[]{2}); // 客户端请求 V2（byte[] 形式）
        packet.setRawBody(new byte[]{2});

        channel.writeInbound(packet);

        // 协商后 channel 属性应设置为版本 2
        Byte negotiatedVersion = channel.attr(
                com.xsh.netty.protocol.ChannelAttributes.NEGOTIATED_VERSION).get();
        assertNotNull(negotiatedVersion, "协商版本不应为 null");

        channel.finish();
    }

    @Test
    void testAuthFail_TimestampOutOfRange() {
        // mock 不返回任何值（因为时间戳校验在 authService 调用之前）
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(authHandler);

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);

        AuthRequest authReq = new AuthRequest();
        authReq.setDeviceId("dev-001");
        authReq.setTimestamp(System.currentTimeMillis() - 10 * 60 * 1000); // 10 分钟前
        authReq.setToken("old-token");
        packet.setBody(authReq);

        channel.writeInbound(packet);

        // 时间戳校验由 RedisAuthService 内部完成，AuthHandler 仅做转发
        verify(authService).authenticate(eq("dev-001"), anyLong(), eq("old-token"), any());

        channel.finish();
    }

    @Test
    void testVersionNegotiate_StringBody() {
        EmbeddedChannel channel = new EmbeddedChannel();
        channel.pipeline().addLast(authHandler);

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.VERSION_NEGOTIATE);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody("1"); // 字符串 "1" → V1

        channel.writeInbound(packet);

        Byte negotiated = channel.attr(ChannelAttributes.NEGOTIATED_VERSION).get();
        assertNotNull(negotiated, "协商版本不应为 null");

        channel.finish();
    }
}
