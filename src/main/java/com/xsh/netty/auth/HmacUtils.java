package com.xsh.netty.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

/**
 * HMAC 签名工具类。
 *
 * <p>统一客户端和服务端的签名算法，确保两端计算结果一致。
 * 客户端和 服务端必须 使用同一个类的同一个方法，避免实现漂移。
 *
 * <p>签名算法：HMAC-MD5(message, secret)
 * <ul>
 *   <li>消息内容：deviceId + timestamp（字符串拼接，timestamp 为毫秒级长整型的十进制字符串）</li>
 *   <li>密钥：设备的 productSecret</li>
 *   <li>输出：32位小写十六进制字符串</li>
 * </ul>
 */
public final class HmacUtils {

    private static final String HMAC_ALGORITHM = "HmacMD5";

    private HmacUtils() {}

    /**
     * 计算 HMAC-MD5 签名。
     *
     * @param deviceId 设备ID
     * @param timestamp 时间戳（毫秒）
     * @param secret    设备密钥（productSecret）
     * @return 32位小写十六进制签名字符串
     */
    public static String computeToken(String deviceId, long timestamp, String secret) {
        String message = deviceId + timestamp;
        return computeHmac(message, secret);
    }

    /**
     * 计算 HMAC-MD5 签名（原始方法）。
     *
     * @param message 消息内容
     * @param secret  密钥
     * @return 32位小写十六进制签名字符串
     */
    public static String computeHmac(String message, String secret) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            SecretKeySpec keySpec = new SecretKeySpec(
                    secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
            mac.init(keySpec);
            byte[] hashBytes = mac.doFinal(message.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hashBytes);
        } catch (Exception e) {
            throw new RuntimeException("HMAC 计算失败", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}
