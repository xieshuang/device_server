package com.xsh.netty.protocol;

import io.netty.util.AttributeKey;

/**
 * Channel 属性键常量定义。
 *
 * <p>通过 {@link io.netty.channel.Channel#attr(AttributeKey)} 绑定/读取属性，
 * 避免在后续报文中重复传输 deviceId 等信息。
 */
public final class ChannelAttributes {

    private ChannelAttributes() {}

    /** 设备ID，鉴权成功后绑定到 Channel */
    public static final AttributeKey<String> DEVICE_ID = AttributeKey.valueOf("deviceId");

    /** 鉴权状态：true=已认证，false=未认证 */
    public static final AttributeKey<Boolean> AUTHENTICATED = AttributeKey.valueOf("authenticated");

    /** 协商后的协议版本号，默认2（V2），V1 客户端协商后设为1 */
    public static final AttributeKey<Byte> NEGOTIATED_VERSION = AttributeKey.valueOf("negotiatedVersion");
}
