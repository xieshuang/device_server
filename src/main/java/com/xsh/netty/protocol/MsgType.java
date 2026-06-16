package com.xsh.netty.protocol;

/**
 * 自定义协议消息类型常量。
 *
 * <p>消息类型定义在协议帧头部的 msgType 字段（1字节）：
 * <ul>
 *   <li>1 - 心跳请求（客户端发送 "PING"）</li>
 *   <li>2 - 心跳响应（服务端回复 "PONG"）</li>
 *   <li>3 - 业务数据</li>
 *   <li>4 - 鉴权请求（设备上线首包，Body 含 deviceId + timestamp + token）</li>
 *   <li>5 - 鉴权成功响应</li>
 *   <li>6 - 鉴权失败响应</li>
 *   <li>7 - 消息确认（ACK，确认收到业务消息）</li>
 * </ul>
 */
public final class MsgType {

    private MsgType() {}

    /** 心跳请求 */
    public static final byte HEARTBEAT_REQ = 1;
    /** 心跳响应 */
    public static final byte HEARTBEAT_RESP = 2;
    /** 业务数据 */
    public static final byte BUSINESS = 3;
    /** 鉴权请求 */
    public static final byte AUTH_REQ = 4;
    /** 鉴权成功响应 */
    public static final byte AUTH_RESP = 5;
    /** 鉴权失败响应 */
    public static final byte AUTH_FAIL = 6;
    /** 消息确认 */
    public static final byte ACK = 7;

    /**
     * 判断是否为心跳类型消息（请求或响应）。
     * 心跳消息的 body 不走序列化，直接按字符串处理。
     */
    public static boolean isHeartbeat(byte msgType) {
        return msgType == HEARTBEAT_REQ || msgType == HEARTBEAT_RESP;
    }
}
