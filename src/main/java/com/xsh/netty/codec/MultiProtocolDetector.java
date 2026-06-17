package com.xsh.netty.codec;

import com.xsh.netty.config.HandlerBeanContainer;
import com.xsh.netty.handler.ModbusBusinessHandler;
import com.xsh.netty.handler.OpcUaBusinessHandler;
import com.xsh.netty.handler.WebSocketBusinessHandler;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 多协议探测器，位于 Pipeline 最前端，通过嗅探数据包的前几个字节判断协议类型，
 * 然后动态替换自身为对应的编解码器。
 *
 * <p>支持的协议：
 * <ul>
 *   <li>自定义协议（魔数 0x44565352 "DVSR"）</li>
 *   <li>HTTP / WebSocket（GET/POST/PUT/DELETE/HEAD/PATCH/OPTIONS）</li>
 *   <li>MQTT（CONNECT 报文类型 0x10）</li>
 * </ul>
 *
 * <p>WebSocket 升级检测：
 * <p>WebSocket 升级请求以 HTTP 格式开头（如 GET /ws HTTP/1.1 + Upgrade: websocket），
 * 但仅凭前4字节无法区分普通 HTTP 和 WebSocket。因此在 HTTP 分支中统一添加
 * HttpServerCodec + HttpObjectAggregator，由后续 Handler 根据请求头中的
 * Upgrade 字段决定走 HTTP 还是 WebSocket 路径。
 *
 * <p>工作原理：使用 {@link ByteBuf#getByte(int)} 读取数据但不移动读指针，
 * 判断协议后通过 {@link io.netty.channel.ChannelPipeline#addAfter} 动态插入编解码器，
 * 最后移除自身，让后续编解码器接管数据处理。
 *
 * <p>无法识别的协议将直接关闭连接，防止恶意扫描。
 */
@Slf4j
public class MultiProtocolDetector extends ByteToMessageDecoder {

    private final NettyServerProperties properties;
    private final HandlerBeanContainer handlerBeanContainer;

    public MultiProtocolDetector(NettyServerProperties properties, HandlerBeanContainer handlerBeanContainer) {
        this.properties = properties;
        this.handlerBeanContainer = handlerBeanContainer;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 可读字节不足4字节，无法判断协议类型，等待更多数据
        if (in.readableBytes() < 4) {
            return;
        }

        // 嗅探前4个字节（不移动读指针），用于协议识别
        final int b0 = in.getByte(in.readerIndex()) & 0xFF;
        final int b1 = in.getByte(in.readerIndex() + 1) & 0xFF;
        final int b2 = in.getByte(in.readerIndex() + 2) & 0xFF;
        final int b3 = in.getByte(in.readerIndex() + 3) & 0xFF;

        String selfName = ctx.name();

        // 1. 自定义协议：魔数 0x44565352 ("DVSR")
        if (isCustomProtocol(b0, b1, b2, b3)) {
            log.debug("检测到自定义协议，来源：{}", ctx.channel().remoteAddress());
            ctx.pipeline().addAfter(selfName, "customDecoder",
                    new CustomProtocolDecoder(properties.getMaxFrameLength()));
            ctx.pipeline().addAfter("customDecoder", "customEncoder", new CustomProtocolEncoder());
            ctx.pipeline().remove(this);
            return;
        }

        // 2. Modbus-TCP 协议：MBAP 头嗅探
        // 字节 2-3 必须为 0x0000 (ProtocolId)，字节 4 必须为 0x00（PDU ≤ 256）
        if (in.readableBytes() >= 7) {
            final int b4 = in.getByte(in.readerIndex() + 4) & 0xFF;
            final int b5 = in.getByte(in.readerIndex() + 5) & 0xFF;
            if (isModbusTcp(b0, b1, b2, b3, b4, b5)) {
                log.debug("检测到 Modbus-TCP 协议，来源：{}", ctx.channel().remoteAddress());
                routeToModbus(ctx, selfName);
                return;
            }
        }

        // 3. OPC-UA TCP Binary 协议：识别 HEL/ACK/OPN/MSG/CLO 消息类型
        if (isOpcUa(b0, b1, b2)) {
            log.debug("检测到 OPC-UA 协议，来源：{}", ctx.channel().remoteAddress());
            routeToOpcUa(ctx, selfName);
            return;
        }

        // 4. HTTP/WebSocket 协议：匹配 GET/POST/PUT/DELETE/HEAD/PATCH/OPTIONS 请求方法
        // WebSocket 升级请求也是 HTTP 格式，先走 HTTP 编解码，后续根据 Upgrade 头区分
        if (isHttp(b0, b1, b2, b3)) {
            log.debug("检测到 HTTP/WebSocket 协议，来源：{}", ctx.channel().remoteAddress());
            ctx.pipeline().addAfter(selfName, "httpCodec", new HttpServerCodec());
            ctx.pipeline().addAfter("httpCodec", "httpAggregator",
                    new HttpObjectAggregator(65536));

            // WebSocket 启用时，添加 WebSocket 协议处理器
            if (properties.isWebsocketEnabled()) {
                ctx.pipeline().addAfter("httpAggregator", "wsProtocolHandler",
                        new WebSocketServerProtocolHandler(
                                properties.getWebsocketPath(), null, true,
                                properties.getWebsocketMaxFrameSize()));
                ctx.pipeline().addAfter("wsProtocolHandler", "wsBusinessHandler",
                        new WebSocketBusinessHandler(handlerBeanContainer));
            }

            ctx.pipeline().remove(this);
            return;
        }

        // 5. MQTT 协议：匹配 CONNECT 报文（首字节高4位=1，即 0x10）
        if (isMqtt(b0, b1)) {
            log.debug("检测到 MQTT 协议，来源：{}", ctx.channel().remoteAddress());
            ctx.pipeline().addAfter(selfName, "mqttDecoder", new MqttDecoder());
            ctx.pipeline().addAfter("mqttDecoder", "mqttEncoder", MqttEncoder.INSTANCE);
            ctx.pipeline().remove(this);
            return;
        }

        // 未知协议，记录失败次数（触发封禁阈值时自动拉黑），然后关闭连接
        log.warn("未知协议，来源：{}，关闭连接", ctx.channel().remoteAddress());
        // 记录 IP 失败次数，超阈值自动封禁
        try {
            String ip = ((java.net.InetSocketAddress) ctx.channel().remoteAddress())
                    .getAddress().getHostAddress();
            handlerBeanContainer.getIpFirewallService().recordFailure(ip);
        } catch (Exception e) {
            log.debug("IP 失败记录异常: {}", e.getMessage());
        }
        in.clear();
        ctx.close();
    }

    /**
     * 判断是否为自定义协议，匹配魔数 "DVSR"（0x44 0x56 0x53 0x52）。
     */
    private boolean isCustomProtocol(int b0, int b1, int b2, int b3) {
        return b0 == 0x44 && b1 == 0x56 && b2 == 0x53 && b3 == 0x52;
    }

    /**
     * 判断是否为 HTTP 协议，匹配常见请求方法的首字母组合。
     *
     * <p>覆盖的请求方法：GET、POST、PUT、HEAD、PATCH、DELETE、OPTIONS
     */
    private boolean isHttp(int b0, int b1, int b2, int b3) {
        return (b0 == 'G' && b1 == 'E' && b2 == 'T') ||                     // GET
               (b0 == 'P' && b1 == 'O' && b2 == 'S' && b3 == 'T') ||        // POST
               (b0 == 'P' && b1 == 'U' && b2 == 'T') ||                     // PUT
               (b0 == 'H' && b1 == 'E' && b2 == 'A' && b3 == 'D') ||        // HEAD
               (b0 == 'P' && b1 == 'A' && b2 == 'T' && b3 == 'C') ||        // PATCH
               (b0 == 'D' && b1 == 'E' && b2 == 'L') ||                     // DELETE
               (b0 == 'O' && b1 == 'P' && b2 == 'T');                       // OPTIONS
    }

    /**
     * 判断是否为 MQTT 协议。
     *
     * <p>MQTT CONNECT 报文特征：
     * <ul>
     *   <li>首字节：高4位为报文类型（1=CONNECT），低4位为标志位</li>
     *   <li>因此 CONNECT 报文首字节高4位 = 1，即 (b0 >> 4) & 0x0F == 1</li>
     *   <li>次字节为剩余长度（Remaining Length）</li>
     * </ul>
     */
    private boolean isMqtt(int b0, int b1) {
        int packetType = (b0 >> 4) & 0x0F;
        return packetType == 1; // MQTT CONNECT
    }

    /**
     * 判断是否为 Modbus-TCP 协议，基于 MBAP 头增强嗅探。
     *
     * <p>MBAP 头 7 字节结构：
     * <pre>
     * TransactionId(2B) | ProtocolId(2B=0x0000) | Length(2B) | UnitId(1B)
     * </pre>
     *
     * <p>V4 增强规则：
     * <ul>
     *   <li>字节 2-3 必须为 0x0000 (ProtocolId = Modbus)</li>
     *   <li>字节 4 必须为 0x00 (工业 Modbus PDU 不超过 256 字节)</li>
     *   <li>防止与自定义协议、变长 HTTP 请求头产生特征重叠误判</li>
     * </ul>
     */
    private boolean isModbusTcp(int b0, int b1, int b2, int b3, int b4, int b5) {
        // ProtocolId 必须为 0x0000
        if (b2 != 0x00 || b3 != 0x00) return false;
        // Length 高字节必须为 0x00（PDU ≤ 256）
        if (b4 != 0x00) return false;
        // Length 低字节 = PDU + 1(UnitId)，范围 (1, 256)
        return b5 > 0 && b5 <= 255;
    }

    /**
     * 动态插入 Modbus-TCP 编解码器链并移除自身。
     */
    private void routeToModbus(ChannelHandlerContext ctx, String selfName) {
        ctx.pipeline().addAfter(selfName, "modbusEncoder", new ModbusEncoder());
        ctx.pipeline().addAfter("modbusEncoder", "modbusDecoder", new ModbusDecoder());
        ctx.pipeline().addAfter("modbusDecoder", "modbusHandler", new ModbusBusinessHandler());
        ctx.pipeline().remove(this);
    }

    /**
     * 判断是否为 OPC-UA TCP Binary 协议。
     *
     * <p>OPC-UA Binary 头部前 3 字节为 ASCII 消息类型：
     * <ul>
     *   <li>HEL — Hello（连接建立）</li>
     *   <li>ACK — Acknowledge（确认）</li>
     *   <li>OPN — OpenSecureChannel（打开安全通道）</li>
     *   <li>MSG — Message（业务消息）</li>
     *   <li>CLO — CloseSecureChannel（关闭安全通道）</li>
     * </ul>
     *
     * <p>第 4 字节为 ChunkType（'F'/'C'/'A'），第 5-8 字节为 MessageSize（uint32 LE）。
     */
    private boolean isOpcUa(int b0, int b1, int b2) {
        return isOpcUaMsgType(b0, b1, b2, 'H', 'E', 'L')   // HEL
            || isOpcUaMsgType(b0, b1, b2, 'A', 'C', 'K')    // ACK
            || isOpcUaMsgType(b0, b1, b2, 'O', 'P', 'N')    // OPN
            || isOpcUaMsgType(b0, b1, b2, 'M', 'S', 'G')    // MSG
            || isOpcUaMsgType(b0, b1, b2, 'C', 'L', 'O')    // CLO
            || isOpcUaMsgType(b0, b1, b2, 'E', 'R', 'R');   // ERR
    }

    private boolean isOpcUaMsgType(int b0, int b1, int b2, char c0, char c1, char c2) {
        return b0 == c0 && b1 == c1 && b2 == c2;
    }

    /**
     * 动态插入 OPC-UA 业务处理器并移除自身。
     */
    private void routeToOpcUa(ChannelHandlerContext ctx, String selfName) {
        ctx.pipeline().addAfter(selfName, "opcUaHandler", new OpcUaBusinessHandler());
        ctx.pipeline().remove(this);
    }
}
