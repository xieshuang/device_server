package com.xsh.netty.protocol;

/**
 * 协议版本信息常量。
 *
 * <p>版本协商流程：
 * <ol>
 *   <li>客户端发送 VERSION_NEGOTIATE 消息（msgType=8），Body 为请求的版本号</li>
 *   <li>服务端比较客户端版本与当前支持的最高版本，取较小值</li>
 *   <li>服务端回复 VERSION_NEGOTIATE，Body 为协商后的版本号</li>
 *   <li>后续通信按协商版本编码/解码</li>
 * </ol>
 *
 * <p>兼容性说明：
 * <ul>
 *   <li>不发送 VERSION_NEGOTIATE 的客户端默认使用 V2（与现有行为一致）</li>
 *   <li>V1 客户端协商后设为版本1，使用11字节头部（无 sequenceId）</li>
 *   <li>客户端版本低于 SERVER_MIN_VERSION 时协商失败，返回 -1</li>
 * </ul>
 */
public final class VersionInfo {

    private VersionInfo() {}

    /** 当前服务端支持的最高协议版本 */
    public static final byte SERVER_MAX_VERSION = 2;

    /** 服务端最低兼容版本 */
    public static final byte SERVER_MIN_VERSION = 1;
}
