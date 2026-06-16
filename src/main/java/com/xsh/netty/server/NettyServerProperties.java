package com.xsh.netty.server;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Netty 服务端配置属性，绑定 application.yml 中 netty.server 前缀的配置项。
 *
 * <p>配置示例：
 * <pre>
 * netty:
 *   server:
 *     port: 9000              # 监听端口
 *     boss-threads: 1         # Acceptor 线程数
 *     worker-threads: 0       # I/O 线程数（0=默认 CPU核心数×2）
 *     business-threads: 64    # 业务处理线程数
 *     idle-timeout-seconds: 5 # 读空闲超时时间（秒）
 *     max-idle-count: 3       # 最大连续空闲次数，超过则断开连接
 *     max-frame-length: 10485760  # 单帧最大长度（字节），防 OOM
 *     so-backlog: 1024        # TCP 连接排队数
 * </pre>
 */
@Data
@Component
@ConfigurationProperties(prefix = "netty.server")
public class NettyServerProperties {

    /** 监听端口 */
    private int port = 9000;
    /** Acceptor 线程数，负责接收新连接，1个线程足够 */
    private int bossThreads = 1;
    /** I/O 线程数，0 表示使用默认值（CPU核心数×2） */
    private int workerThreads = 0;
    /** 业务处理线程池大小，独立于 I/O 线程 */
    private int businessThreads = 64;
    /** 读空闲超时时间（秒），超时触发 IdleStateEvent */
    private int idleTimeoutSeconds = 5;
    /** 最大连续空闲次数，达到后断开连接，避免网络抖动误杀 */
    private int maxIdleCount = 3;
    /** 单帧最大允许长度（字节），超过将关闭连接，防止恶意客户端导致 OOM */
    private int maxFrameLength = 10485760; // 10MB
    /** TCP SO_BACKLOG 参数，允许排队的连接数 */
    private int soBacklog = 1024;

    /** TLS 是否启用 */
    private boolean tlsEnabled = false;
    /** TLS 独立端口（与明文端口分离） */
    private int tlsPort = 9001;
    /** TLS 证书路径（PKCS12/JKS） */
    private String tlsCertPath;
    /** TLS 证书密码 */
    private String tlsCertPassword;
    /** 鉴权超时时间（秒），连接建立后超过此时间未鉴权则断开 */
    private int authTimeoutSeconds = 10;

    // ---- Kafka 配置 ----
    /** Kafka 消息持久化开关，false 时 BusinessMessageHandler 仅打日志 */
    private boolean kafkaEnabled = false;
    /** Kafka 统一 Topic 名称 */
    private String kafkaTopic = "device-messages";

    // ---- 限流配置 ----
    /** 限流开关 */
    private boolean rateLimitEnabled = true;
    /** 全局每秒允许的消息数（令牌桶速率） */
    private double rateLimitGlobalPermits = 10000.0;
    /** 单设备每秒允许的消息数 */
    private double rateLimitDevicePermits = 100.0;
    /** 限流后是否关闭连接（false=丢弃消息保持连接，true=直接关闭） */
    private boolean rateLimitCloseOnLimit = false;

    // ---- WebSocket 配置 ----
    /** WebSocket 开关 */
    private boolean websocketEnabled = true;
    /** WebSocket 升级路径 */
    private String websocketPath = "/ws";
    /** WebSocket 最大帧大小 */
    private int websocketMaxFrameSize = 65536;
}
