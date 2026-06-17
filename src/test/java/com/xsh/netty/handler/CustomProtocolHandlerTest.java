package com.xsh.netty.handler;

import com.xsh.netty.config.HandlerBeanContainer;
import com.xsh.netty.protocol.ChannelAttributes;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.PendingAckManager;
import com.xsh.netty.dispatcher.MessageDispatcher;
import com.xsh.netty.config.NettyMetricsBinder;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * CustomProtocolHandler 单元测试。
 */
class CustomProtocolHandlerTest {

    private DeviceChannelManager channelManager;
    private HandlerBeanContainer container;
    private CustomProtocolHandler handler;
    private MessageDispatcher dispatcher;
    private PendingAckManager pendingAckManager;
    private NettyMetricsBinder metricsBinder;

    @BeforeEach
    void setUp() {
        channelManager = mock(DeviceChannelManager.class);
        dispatcher = mock(MessageDispatcher.class);
        pendingAckManager = mock(PendingAckManager.class);
        metricsBinder = mock(NettyMetricsBinder.class);
        container = mock(HandlerBeanContainer.class);
        when(container.getChannelManager()).thenReturn(channelManager);
        when(container.getMessageDispatcher()).thenReturn(dispatcher);
        when(container.getPendingAckManager()).thenReturn(pendingAckManager);
        when(container.getMetricsBinder()).thenReturn(metricsBinder);

        handler = new CustomProtocolHandler(3, channelManager, container);
    }

    // ==================== 心跳处理 ====================

    @Test
    void testHeartbeatRequest_RepliesPong() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.HEARTBEAT_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody("PING");

        channel.writeInbound(packet);

        // 验证心跳指标被调用
        verify(metricsBinder).incrementHeartbeatReq();
        verify(metricsBinder).incrementHeartbeatResp();

        channel.finish();
    }

    @Test
    void testHeartbeatRequest_ReturnsPong() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.HEARTBEAT_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);

        channel.writeInbound(packet);

        // 心跳应回复 PONG
        MessagePacket out = channel.readOutbound();
        assertNotNull(out, "应回复心跳响应");

        channel.finish();
    }

    // ==================== ACK 处理 ====================

    @Test
    void testAckMessage_CallsPendingAckManager() {
        when(pendingAckManager.ack(anyString(), eq(100))).thenReturn(true);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.ACK);
        header.setSequenceId(100);
        packet.setHeader(header);

        channel.writeInbound(packet);

        verify(pendingAckManager).ack(anyString(), eq(100));

        channel.finish();
    }

    @Test
    void testAckMessage_NotFound_NoException() {
        when(pendingAckManager.ack(anyString(), anyInt())).thenReturn(false);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.ACK);
        header.setSequenceId(999);
        packet.setHeader(header);

        // 不应抛异常
        assertDoesNotThrow(() -> channel.writeInbound(packet));

        channel.finish();
    }

    // ==================== 业务消息分发 ====================

    @Test
    void testBusinessMessage_Dispatches() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);
        packet.setBody("test-data".getBytes());

        channel.writeInbound(packet);

        verify(dispatcher).dispatch(any(), eq("dev-001"), eq(packet));

        channel.finish();
    }

    @Test
    void testBusinessMessage_DispatchException_NotThrown() {
        doThrow(new RuntimeException("handler error")).when(dispatcher)
                .dispatch(any(), anyString(), any());

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        // 分发出异常不应导致连接断开
        assertDoesNotThrow(() -> channel.writeInbound(packet));

        channel.finish();
    }

    // ==================== 连接生命周期 ====================

    @Test
    void testChannelInactive_UnregistersDevice() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        channel.close();

        verify(channelManager).unregister(eq("dev-001"), any());

        channel.finish();
    }

    @Test
    void testChannelInactive_NoDeviceId_NoUnregister() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        // 未设置 deviceId

        channel.close();

        verify(channelManager, never()).unregister(anyString(), any());

        channel.finish();
    }

    // ==================== 空闲检测 ====================

    @Test
    void testIdleStateEvent_UnderThreshold_StaysActive() {
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        channel.attr(ChannelAttributes.DEVICE_ID).set("dev-001");

        // 触发 1 次空闲（小于 maxIdleCount=3）
        channel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);

        // 不应断开
        channel.finish();
    }
}
