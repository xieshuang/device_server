package com.xsh.netty.config;

import com.xsh.netty.auth.AuthService;
import com.xsh.netty.codec.MultiProtocolDetector;
import com.xsh.netty.handler.AuthHandler;
import com.xsh.netty.handler.BackpressureHandler;
import com.xsh.netty.handler.CustomProtocolHandler;
import com.xsh.netty.handler.HttpBusinessHandler;
import com.xsh.netty.handler.IpFilterHandler;
import com.xsh.netty.handler.MqttBusinessHandler;
import com.xsh.netty.ratelimit.RateLimitHandler;
import com.xsh.netty.ratelimit.RateLimiterService;
import com.xsh.netty.server.DeviceChannelManager;
import com.xsh.netty.server.IpFirewallService;
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
    private final IpFirewallService ipFirewallService;

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
        // V4 强防御：集群模式启用时 node-id 严禁留空
        verifyClusterConfig();
        
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
                // 背压流控高低水位配置（写缓冲区达到高水位时触发 channelWritabilityChanged）
                .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
                        properties.getBackpressureHighWaterMark())
                .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
                        properties.getBackpressureLowWaterMark())
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 0. TLS（如果启用且当前为 TLS 端口）
                        if (sslContext != null && ch.localAddress().getPort() == properties.getTlsPort()) {
                            pipeline.addLast(sslContext.newHandler(ch.alloc()));
                        }

                        // 0.5 IP 黑名单过滤：三次握手后就绪，拒绝恶意 IP 接入
                        if (properties.isIpFilterEnabled()) {
                            pipeline.addLast(new IpFilterHandler(ipFirewallService));
                        }

                        // 1. 鉴权超时：连接建立后超过此时间未发送鉴权请求则断开
                        pipeline.addLast(new ReadTimeoutHandler(properties.getAuthTimeoutSeconds(), TimeUnit.SECONDS));

                        // 1.5 TCP 背压流控：基于 channelWritabilityChanged 双向驱动，反馈设备降频
                        if (properties.isBackpressureEnabled()) {
                            pipeline.addLast(new BackpressureHandler());
                        }

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
     *
     * <p>停机流程：
     * <ol>
     *   <li>关闭监听端口，拒绝新连接</li>
     *   <li>向所有在线设备广播维护通知</li>
     *   <li>冲刷 Kafka 缓冲器（确保遗言投递）</li>
     *   <li>关闭所有设备连接</li>
     *   <li>释放 Netty 线程组资源</li>
     * </ol>
     */
    @PreDestroy
    public void stop() {
        log.info("================== 启动网关生产级优雅停机流程 ==================");
        // 0. 关闭监听端口，拒绝任何新的物理连接连入
        if (serverChannel != null) { serverChannel.close().syncUninterruptibly(); }
        if (tlsServerChannel != null) { tlsServerChannel.close().syncUninterruptibly(); }

        try {
            // 1. 下发系统维护宣告消息给物理设备
            channelManager.broadcastMaintenanceNotice();

            // 2. 冲刷 Kafka 缓冲器，确保异步遗言持久化入盘
            if (handlerBeanContainer.getKafkaProducerService() != null) {
                handlerBeanContainer.getKafkaProducerService().flushBuffer(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.error("优雅停机业务善后阶段遭遇异常: ", e);
        } finally {
            // 3. 强行下线清理本地全部残存 Channel 句柄
            channelManager.closeAll();

            // 4. 优雅释放 Netty 各层级线程组资源
            if (bossGroup != null) bossGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS);
            if (workerGroup != null) workerGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS);
            if (businessGroup != null) businessGroup.shutdownGracefully(2, 5, TimeUnit.SECONDS);
            log.info("================== 网关资源释放完毕，安全退出进程 ==================");
        }
    }

    /** 安全关闭 Channel，异常不传递 */
    private void silentClose(Channel channel, String name) {
        if (channel != null) {
            try {
                channel.close().awaitUninterruptibly();
            } catch (Exception e) {
                log.error("关闭 {} 时异常: {}", name, e.getMessage());
            }
        }
    }

    /** 安全关闭 EventLoopGroup，异常不传递 */
    private void silentShutdown(EventExecutorGroup group, String name) {
        if (group != null) {
            try {
                group.shutdownGracefully();
            } catch (Exception e) {
                log.error("关闭 {} 时异常: {}", name, e.getMessage());
            }
        }
    }

    /**
     * V4 强防御：集群模式启用时校验 node-id 不可为空。
     */
    private void verifyClusterConfig() {
        if (properties.isClusterEnabled()) {
            String nodeId = properties.getClusterNodeId();
            if (nodeId == null || nodeId.trim().isEmpty()) {
                log.error("=================================================================");
                log.error(" 核心启动灾难错误: 网关已开启集群功能，但 netty.server.cluster-node-id 处于留空状态！");
                log.error(" 系统拒绝启动以拦截未知时序路由崩溃风险。");
                log.error("=================================================================");
                throw new IllegalStateException("分布式集群配置节点标识异常：cluster-node-id 缺失");
            }
            log.info("集群节点标识校验通过: nodeId={}", nodeId);
        }
    }
}
