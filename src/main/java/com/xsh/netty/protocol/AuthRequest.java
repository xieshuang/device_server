package com.xsh.netty.protocol;

import lombok.Data;

/**
 * 鉴权请求体，由 AUTH_REQ 消息的 Body 反序列化而来。
 *
 * <p>鉴权流程：
 * <ol>
 *   <li>设备出厂时烧录 ProductSecret</li>
 *   <li>设备上电时生成 token = HMAC-MD5(deviceId + timestamp, productSecret)</li>
 *   <li>首包发送 AUTH_REQ，Body 为本对象的 JSON 序列化</li>
 *   <li>服务端从 Redis 读取该 deviceId 对应的 productSecret，验证 token</li>
 * </ol>
 */
@Data
public class AuthRequest {

    /** 设备唯一标识 */
    private String deviceId;

    /** 时间戳（毫秒），用于防重放攻击 */
    private long timestamp;

    /** 签名令牌：HMAC-MD5(deviceId + timestamp, productSecret) */
    private String token;

    /** 防重放 nonce（V5 新增）：客户端生成的随机字符串，服务端通过 Redis SETNX 校验。
     *  旧客户端不发送此字段时为 null，降级为仅 timestamp 校验 */
    private String nonce;
}
