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
}
