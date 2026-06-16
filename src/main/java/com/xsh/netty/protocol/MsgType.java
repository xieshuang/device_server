package com.xsh.netty.protocol;

/**
 * 自定义协议消息类型常量。
 *
 * <p>消息类型定义在协议帧头部的 msgType 字段（1字节）：
 * <ul>
 *   <li>1 - 心跳请求（客户端发送 "PING"）</li>
 *   <li>2 - 心跳响应（服务端回复 "PONG"）</li>
 *   <li>3 - 业务数据</li>
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

    /**
     * 判断是否为心跳类型消息（请求或响应）。
     * 心跳消息的 body 不走序列化，直接按字符串处理。
     */
    public static boolean isHeartbeat(byte msgType) {
        return msgType == HEARTBEAT_REQ || msgType == HEARTBEAT_RESP;
    }
}
