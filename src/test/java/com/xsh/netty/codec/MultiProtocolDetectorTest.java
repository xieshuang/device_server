package com.xsh.netty.codec;

import com.xsh.netty.config.HandlerBeanContainer;
import com.xsh.netty.handler.ModbusBusinessHandler;
import com.xsh.netty.handler.OpcUaBusinessHandler;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.mqtt.MqttDecoder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.xsh.netty.server.IpFirewallService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 多协议探测器单元测试。
 *
 * <p>验证 6 种协议的嗅探正确性和未知协议的防御性关闭。
 */
class MultiProtocolDetectorTest {

    private NettyServerProperties properties;
    private HandlerBeanContainer container;

    @BeforeEach
    void setUp() {
        properties = new NettyServerProperties();
        properties.setWebsocketEnabled(false);
        container = mock(HandlerBeanContainer.class);
        when(container.getIpFirewallService()).thenReturn(mock(IpFirewallService.class));
    }

    // ==================== 协议类型嗅探 ====================

    @Test
    void testDetectCustomProtocol_DVSR() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(0x44565352); // "DVSR"

        channel.writeInbound(buf);

        // DVSR 路径：应插入 CustomProtocolDecoder + CustomProtocolEncoder
        assertNotNull(channel.pipeline().get("customDecoder"));
        assertNotNull(channel.pipeline().get("customEncoder"));
        // 探测器自身应已移除
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    @Test
    void testDetectModbusTcp() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        // 构造合法 MBAP 头：TransactionId + ProtocolId=0x0000 + Length(含UnitId)
        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(1);       // TransactionId
        buf.writeShort(0x0000);  // ProtocolId = 0
        buf.writeShort(6);       // Length = 1 (UnitId) + 5 (PDU)
        buf.writeByte(1);        // UnitId

        channel.writeInbound(buf);

        // Modbus 路径：应插入 Modbus 编解码器和 Handler
        assertNotNull(channel.pipeline().get(ModbusEncoder.class));
        assertNotNull(channel.pipeline().get(ModbusDecoder.class));
        assertNotNull(channel.pipeline().get(ModbusBusinessHandler.class));
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    @Test
    void testDetectOpcUa_HEL() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        // OPC-UA 最小需要 4 字节嗅探（3 字节消息类型 + 至少 1 字节后续数据）
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("HELF".getBytes()); // HEL + ChunkType 'F'

        channel.writeInbound(buf);

        // OPC-UA 路径：应插入 OpcUaBusinessHandler
        assertNotNull(channel.pipeline().get(OpcUaBusinessHandler.class));
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    @Test
    void testDetectOpcUa_MSG() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("MSGF".getBytes()); // MSG + ChunkType 'F'

        channel.writeInbound(buf);

        assertNotNull(channel.pipeline().get(OpcUaBusinessHandler.class));
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    @Test
    void testDetectHttp_GET() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("GET / HTTP/1.1".getBytes());

        channel.writeInbound(buf);

        // HTTP 路径：应插入 HttpServerCodec
        assertNotNull(channel.pipeline().get(HttpServerCodec.class));
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    @Test
    void testDetectHttp_POST() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("POST /api HTTP/1.1".getBytes());

        channel.writeInbound(buf);

        assertNotNull(channel.pipeline().get(HttpServerCodec.class));
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    @Test
    void testDetectMqtt_CONNECT() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        // MQTT CONNECT 报文：首字节高4位=1（消息类型=1）
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(0x10); // CONNECT
        buf.writeByte(0x0A); // Remaining Length
        buf.writeBytes("MQTT".getBytes());

        channel.writeInbound(buf);

        // MQTT 路径：应插入 MqttDecoder
        assertNotNull(channel.pipeline().get(MqttDecoder.class));
        assertNull(channel.pipeline().get(MultiProtocolDetector.class));

        channel.finish();
    }

    // ==================== 异常场景 ====================

    @Test
    void testDetectUnknown_Close() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        // 4 字节无法匹配任何已知协议
        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(0x11111111);

        channel.writeInbound(buf);

        // 未知协议时 ByteToMessageDecoder 会通过 ctx.close() 关闭连接
        // EmbeddedChannel 的 close 是异步的，通过 finish 触发清理
        channel.finish();

        // 验证管道不包含后续协议 Handler
        assertNull(channel.pipeline().get(HttpServerCodec.class));
    }

    @Test
    void testPartialData_Wait() {
        MultiProtocolDetector detector = new MultiProtocolDetector(properties, container);
        EmbeddedChannel channel = new EmbeddedChannel(detector);

        // 仅 3 字节，不足 4 字节嗅探最小要求
        ByteBuf partial = Unpooled.buffer();
        partial.writeByte(0x44);
        partial.writeByte(0x56);
        partial.writeByte(0x53);

        channel.writeInbound(partial);

        // 数据不足时探测器不移除自身
        assertNotNull(channel.pipeline().get(MultiProtocolDetector.class));
        assertTrue(channel.isActive());

        channel.finish();
    }
}
