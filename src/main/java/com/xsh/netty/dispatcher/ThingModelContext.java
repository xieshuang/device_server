package com.xsh.netty.dispatcher;

import com.xsh.netty.protocol.MessagePacket;
import io.netty.channel.Channel;
import lombok.Getter;

/**
 * 物模型上下文，封装设备原始数据及关联元信息。
 *
 * <p>作为 {@link ThingModelMessageHandler#convertToThingModel} 的输入参数，
 * 提供设备标识、原始二进制数据、序列化类型和连接通道等上下文信息。
 */
@Getter
public class ThingModelContext {

    /** 设备ID */
    private final String deviceId;

    /** 原始消息包（含 rawBody 和 header 元信息） */
    private final MessagePacket packet;

    /** 是否来自 Modbus-TCP 协议（区别于面向通道） */
    private final boolean fromModbus;

    /** 是否来自 OPC-UA 协议 */
    private final boolean fromOpcUa;

    public ThingModelContext(String deviceId, MessagePacket packet) {
        this(deviceId, packet, false, false);
    }

    public ThingModelContext(String deviceId, MessagePacket packet,
                              boolean fromModbus, boolean fromOpcUa) {
        this.deviceId = deviceId;
        this.packet = packet;
        this.fromModbus = fromModbus;
        this.fromOpcUa = fromOpcUa;
    }

    /**
     * 获取原始字节数组（优先 rawBody，其次 body）。
     */
    public byte[] getRawBytes() {
        if (packet.getRawBody() != null) {
            return packet.getRawBody();
        }
        if (packet.getBody() instanceof byte[] bytes) {
            return bytes;
        }
        return null;
    }

    /**
     * 获取序列化类型（1=JSON, 2=Protobuf）。
     */
    public byte getSerializationType() {
        return packet.getHeader().getSerializationType();
    }

    /**
     * 获取消息序列号。
     */
    public int getSequenceId() {
        return packet.getHeader().getSequenceId();
    }
}
