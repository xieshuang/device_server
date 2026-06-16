package com.xsh.netty.config;

import com.xsh.netty.auth.AuthService;
import com.xsh.netty.codec.MultiProtocolDetector;
import com.xsh.netty.handler.AuthHandler;
import com.xsh.netty.handler.CustomProtocolHandler;
import com.xsh.netty.handler.HttpBusinessHandler;
import com.xsh.netty.handler.MqttBusinessHandler;
import com.xsh.netty.ratelimit.RateLimitHandler;
import com.xsh.netty.ratelimit.RateLimiterService;
import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Netty 服务端启动引导类，由 Spring Boot 管理生命周期。
 *
 * <p>核心设计：
 * <ul>
 *   <li>自动检测并启用 Epoll（Linux 环境性能更优），否则回退到 NIO</li>
 *   <li>线程模型：bossGroup(1线程接收连接) → workerGroup(I/O读写) → businessGroup(业务处理)</li>
 *   <li>Pipeline 流程：[SslHandler] → ReadTimeoutHandler → IdleStateHandler → MultiProtocolDetector
 *       → AuthHandler → RateLimitHandler → CustomProtocolHandler</li>
 *   <li>业务 Handler 在独立线程池中执行，防止阻塞 I/O 线程</li>
 *   <li>Spring 容器关闭时优雅停机，释放所有资源</li>
 * </ul>
 *
 * <p>Pipeline 动态路由流程（自定义协议路径）：
 * <pre>
 * [ByteBuf] → [SslHandler(可选)] → [ReadTimeoutHandler] → [IdleStateHandler]
 *     → [MultiProtocolDetector] → [CustomDecoder] → [AuthHandler] → [RateLimitHandler]
 *                                                           │                │
 *                                                  鉴权成功后移除 AuthHandler  限流检查
 *                                                                           ↓
 *                                                              [CustomProtocolHandler]
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NettyServerBootstrap {

    private final NettyServerProperties properties;
    private final AuthService authService;
    private final DeviceChannelManager channelManager;
    private final HandlerBeanContainer handlerBeanContainer;

    /** Acceptor 线程组，负责接收新连接（1个线程足够） */
    private EventLoopGroup bossGroup;
    /** I/O 线程组，负责读写数据（默认 CPU核心数×2） */
    private EventLoopGroup workerGroup;
    /** 业务处理线程组，独立于 I/O 线程，避免阻塞事件循环 */
    private EventExecutorGroup businessGroup;
    /** 明文监听 Channel */
    private Channel serverChannel;
    /** TLS 监听 Channel */
    private Channel tlsServerChannel;
    /** SSL 上下文 */
    private SslContext sslContext;

    /**
     * Spring 容器初始化后启动 Netty 服务。
     */
    @PostConstruct
    public void start() throws Exception {
        boolean useEpoll = Epoll.isAvailable();
        log.info("Netty 服务启动中, Epoll 可用: {}", useEpoll);

        // 初始化 SSL 上下文
        if (properties.isTlsEnabled()) {
            sslContext = buildSslContext();
            log.info("TLS 已启用，证书配置完成");
        }

        // 根据平台选择 EventLoopGroup 实现
        bossGroup = useEpoll
                ? new EpollEventLoopGroup(properties.getBossThreads())
                : new NioEventLoopGroup(properties.getBossThreads());

        workerGroup = useEpoll
                ? new EpollEventLoopGroup(properties.getWorkerThreads())
                : new NioEventLoopGroup(properties.getWorkerThreads());

        businessGroup = new DefaultEventExecutorGroup(properties.getBusinessThreads());

        // 启动明文端口
        ServerBootstrap bootstrap = createServerBootstrap(useEpoll);
        ChannelFuture future = bootstrap.bind(properties.getPort()).sync();
        serverChannel = future.channel();
        log.info("Netty 明文服务启动，端口: {} (Epoll: {})", properties.getPort(), useEpoll);

        // 启动 TLS 端口
        if (properties.isTlsEnabled()) {
            ServerBootstrap tlsBootstrap = createServerBootstrap(useEpoll);
            // 在 childHandler 中最前端加入 SslHandler
            ChannelFuture tlsFuture = tlsBootstrap.bind(properties.getTlsPort()).sync();
            tlsServerChannel = tlsFuture.channel();
            log.info("Netty TLS 服务启动，端口: {}", properties.getTlsPort());
        }
    }

    /**
     * 创建 ServerBootstrap，配置 Pipeline。
     */
    private ServerBootstrap createServerBootstrap(boolean useEpoll) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, properties.getSoBacklog())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 0. TLS（如果启用且当前为 TLS 端口）
                        if (sslContext != null && ch.localAddress().getPort() == properties.getTlsPort()) {
                            pipeline.addLast(sslContext.newHandler(ch.alloc()));
                        }

                        // 1. 鉴权超时：连接建立后超过此时间未发送鉴权请求则断开
                        pipeline.addLast(new ReadTimeoutHandler(properties.getAuthTimeoutSeconds(), TimeUnit.SECONDS));

                        // 2. 空闲状态检测：指定时间内无读数据则触发 IdleStateEvent
                        pipeline.addLast(new IdleStateHandler(
                                properties.getIdleTimeoutSeconds(), 0, 0, TimeUnit.SECONDS));

                        // 3. 多协议探测器：嗅探前几个字节判断协议类型，动态替换自身为对应编解码器
                        pipeline.addLast(new MultiProtocolDetector(properties, handlerBeanContainer));

                        // 4. 鉴权 Handler：未认证连接只允许 AUTH_REQ，鉴权成功后自动移除
                        pipeline.addLast(businessGroup, "authHandler",
                                new AuthHandler(authService, channelManager,
                                        handlerBeanContainer.getMetricsBinder()));

                        // 4.5 限流 Handler（鉴权通过后生效，心跳不限流）
                        if (properties.isRateLimitEnabled()) {
                            pipeline.addLast(businessGroup, "rateLimitHandler",
                                    new RateLimitHandler(handlerBeanContainer.getRateLimiterService(), properties));
                        }

                        // 5. 业务处理器：放入独立线程池执行，避免阻塞 I/O 线程
                        pipeline.addLast(businessGroup, "customHandler",
                                new CustomProtocolHandler(properties.getMaxIdleCount(), channelManager,
                                        handlerBeanContainer));
                        pipeline.addLast(businessGroup, "httpHandler", new HttpBusinessHandler());
                        pipeline.addLast(businessGroup, "mqttHandler", new MqttBusinessHandler());
                    }
                });
        return bootstrap;
    }

    /**
     * 构建 SSL 上下文。
     */
    private SslContext buildSslContext() throws Exception {
        if (properties.getTlsCertPath() != null && !properties.getTlsCertPath().isBlank()) {
            // 使用指定证书
            File certFile = new File(properties.getTlsCertPath());
            if (!certFile.exists()) {
                throw new IllegalStateException("TLS 证书文件不存在: " + properties.getTlsCertPath());
            }
            return SslContextBuilder.forServer(certFile, certFile, properties.getTlsCertPassword())
                    .build();
        } else {
            // 开发环境：自签名证书
            log.warn("未配置 TLS 证书，使用自签名证书（仅限开发环境）");
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            return SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey())
                    .build();
        }
    }

    /**
     * Spring 容器关闭时优雅停机。
     */
    @PreDestroy
    public void stop() {
        log.info("Netty 服务关闭中...");
        if (tlsServerChannel != null) {
            tlsServerChannel.close();
        }
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        if (businessGroup != null) {
            businessGroup.shutdownGracefully();
        }
        log.info("Netty 服务已关闭");
    }
}
