package com.xsh.netty.auth;

import java.util.concurrent.CompletableFuture;

/**
 * 设备鉴权服务接口。
 *
 * <p>负责验证设备的鉴权请求，核心逻辑：
 * <ol>
 *   <li>从 Redis 异步读取 deviceId 对应的 productSecret</li>
 *   <li>使用 productSecret 计算 HMAC-MD5(deviceId + timestamp)</li>
 *   <li>比对计算结果与客户端传来的 token</li>
 * </ol>
 */
public interface AuthService {

    /**
     * 异步验证设备鉴权请求。
     *
     * @param deviceId  设备ID
     * @param timestamp 时间戳（毫秒）
     * @param token     客户端计算的 HMAC 签名
     * @return CompletableFuture，true=鉴权通过，false=鉴权失败
     */
    CompletableFuture<Boolean> authenticate(String deviceId, long timestamp, String token);
}
