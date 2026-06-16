package com.xsh.netty.protocol;

import lombok.extern.slf4j.Slf4j;

/**
 * 协议版本协商器，根据客户端请求版本和服务端支持版本确定最终通信版本。
 *
 * <p>协商策略：取客户端版本和服务端最高版本的最小值（min(clientVersion, SERVER_MAX_VERSION)）。
 *
 * <p>线程安全：无状态，纯函数式，可并发调用。
 *
 * <p>示例：
 * <pre>
 * negotiate((byte) 2) → 2  // V2 客户端，协商结果 V2
 * negotiate((byte) 1) → 1  // V1 客户端，降级到 V1
 * negotiate((byte) 3) → 2  // V3 客户端，服务端最高支持 V2，降级到 V2
 * negotiate((byte) 0) → -1 // V0 客户端，低于最低兼容版本，协商失败
 * </pre>
 */
@Slf4j
public final class VersionNegotiator {

    private VersionNegotiator() {}

    /**
     * 协商协议版本。
     *
     * @param clientVersion 客户端请求的协议版本
     * @return 协商后的版本号，-1 表示不兼容
     */
    public static byte negotiate(byte clientVersion) {
        // 客户端版本低于最低兼容版本，协商失败
        if (clientVersion < VersionInfo.SERVER_MIN_VERSION) {
            log.warn("客户端版本过低: {}, 最低支持: {}", clientVersion, VersionInfo.SERVER_MIN_VERSION);
            return -1;
        }

        // 取客户端和服务端最高版本的较小值
        byte negotiated = (byte) Math.min(clientVersion, VersionInfo.SERVER_MAX_VERSION);
        if (negotiated < clientVersion) {
            log.info("协议版本降级: 客户端请求={}, 协商结果={}", clientVersion, negotiated);
        }
        return negotiated;
    }
}
