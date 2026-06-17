package com.xsh.netty.server;

import com.xsh.netty.protocol.ChannelAttributes;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 设备连接管理器单元测试。
 */
class DeviceChannelManagerTest {

    private final NettyServerProperties properties = new NettyServerProperties();

    @Test
    void testRegister_NewDevice() {
        DeviceChannelManager manager = new DeviceChannelManager();
        EmbeddedChannel channel = new EmbeddedChannel();

        manager.register("dev-001", channel);
        assertEquals(1, manager.getOnlineCount());
        assertNotNull(manager.getChannel("dev-001"));

        channel.finish();
    }

    @Test
    void testRegister_DuplicateKickOld() {
        DeviceChannelManager manager = new DeviceChannelManager();
        EmbeddedChannel oldChannel = new EmbeddedChannel();
        EmbeddedChannel newChannel = new EmbeddedChannel();

        manager.register("dev-001", oldChannel);
        manager.register("dev-001", newChannel);

        // 旧连接应被关闭
        assertFalse(oldChannel.isActive());
        // 新连接应注册成功
        assertNotNull(manager.getChannel("dev-001"));

        oldChannel.finish();
        newChannel.finish();
    }

    @Test
    void testUnregister_MatchingChannel() {
        DeviceChannelManager manager = new DeviceChannelManager();
        EmbeddedChannel channel = new EmbeddedChannel();

        manager.register("dev-001", channel);
        assertEquals(1, manager.getOnlineCount());

        manager.unregister("dev-001", channel);
        assertEquals(0, manager.getOnlineCount());

        channel.finish();
    }

    @Test
    void testUnregister_StaleChannel_NotRemoved() {
        DeviceChannelManager manager = new DeviceChannelManager();
        EmbeddedChannel channel1 = new EmbeddedChannel();
        EmbeddedChannel channel2 = new EmbeddedChannel();

        manager.register("dev-001", channel1);
        // 尝试用不同的 Channel 注销 → 不应移除
        manager.unregister("dev-001", channel2);
        assertEquals(1, manager.getOnlineCount());

        channel1.finish();
        channel2.finish();
    }

    @Test
    void testSendToDevice_Online() {
        DeviceChannelManager manager = new DeviceChannelManager();
        EmbeddedChannel channel = new EmbeddedChannel();

        manager.register("dev-001", channel);
        assertTrue(manager.sendToDevice("dev-001", "hello"));

        Object msg = channel.readOutbound();
        assertEquals("hello", msg);

        channel.finish();
    }

    @Test
    void testSendToDevice_Offline() {
        DeviceChannelManager manager = new DeviceChannelManager();
        assertFalse(manager.sendToDevice("offline-device", "hello"));
    }

    @Test
    void testGetOnlineCount_PreservesAccuracy() {
        DeviceChannelManager manager = new DeviceChannelManager();

        assertEquals(0, manager.getOnlineCount());

        EmbeddedChannel ch1 = new EmbeddedChannel();
        EmbeddedChannel ch2 = new EmbeddedChannel();
        manager.register("dev-A", ch1);
        manager.register("dev-B", ch2);
        assertEquals(2, manager.getOnlineCount());

        manager.unregister("dev-A", ch1);
        assertEquals(1, manager.getOnlineCount());

        ch1.finish();
        ch2.finish();
    }

    @Test
    void testGetChannel_NotRegistered() {
        DeviceChannelManager manager = new DeviceChannelManager();
        assertNull(manager.getChannel("nonexistent"));
    }
}
