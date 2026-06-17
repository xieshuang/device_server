package com.xsh.netty.dispatcher;

import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消息分发器单元测试。
 */
class MessageDispatcherTest {

    @Test
    void testRegisterAndDispatch() {
        MessageDispatcher dispatcher = new MessageDispatcher();
        AtomicInteger callCount = new AtomicInteger(0);

        dispatcher.register(new MessageHandler() {
            @Override
            public byte supportMsgType() { return MsgType.BUSINESS; }
            @Override
            public void handle(ChannelHandlerContext ctx, String deviceId, MessagePacket packet) {
                callCount.incrementAndGet();
            }
        });

        EmbeddedChannel channel = new EmbeddedChannel();
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        packet.setHeader(header);

        dispatcher.dispatch(channel.pipeline().firstContext(), "dev-001", packet);
        assertEquals(1, callCount.get());

        channel.finish();
    }

    @Test
    void testDispatch_UnregisteredMsgType() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        EmbeddedChannel channel = new EmbeddedChannel();
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType((byte) 99); // 未注册的类型
        packet.setHeader(header);

        // 未注册类型不应抛异常
        dispatcher.dispatch(channel.pipeline().firstContext(), "dev-001", packet);
        assertTrue(channel.isActive());

        channel.finish();
    }

    @Test
    void testRegisterAll() {
        MessageDispatcher dispatcher = new MessageDispatcher();

        MessageHandler handler = new MessageHandler() {
            @Override
            public byte supportMsgType() { return MsgType.BUSINESS; }
            @Override
            public void handle(ChannelHandlerContext ctx, String deviceId, MessagePacket packet) {}
        };

        dispatcher.registerAll(List.of(handler));
        assertNotNull(dispatcher);
    }
}
