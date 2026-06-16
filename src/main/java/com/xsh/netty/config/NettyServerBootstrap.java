package com.xsh.netty.config;

import com.xsh.netty.codec.MultiProtocolDetector;
import com.xsh.netty.handler.CustomProtocolHandler;
import com.xsh.netty.handler.HttpBusinessHandler;
import com.xsh.netty.handler.MqttBusinessHandler;
import com.xsh.netty.server.NettyServerProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Netty 服务端启动引导类，由 Spring Boot 管理生命周期。
 *
 * <p>核心设计：
 * <ul>
 *   <li>自动检测并启用 Epoll（Linux 环境性能更优），否则回退到 NIO</li>
 *   <li>线程模型：bossGroup(1线程接收连接) → workerGroup(I/O读写) → businessGroup(业务处理)</li>
 *   <li>Pipeline 流程：IdleStateHandler → MultiProtocolDetector → 业务Handler</li>
 *   <li>业务 Handler 在独立线程池中执行，防止阻塞 I/O 线程</li>
 *   <li>Spring 容器关闭时优雅停机，释放所有资源</li>
 * </ul>
 *
 * <p>Pipeline 动态路由流程：
 * <pre>
 * [ByteBuf] → [IdleStateHandler] → [MultiProtocolDetector]
 *                                       │
 *                    ┌──────────────────┼──────────────────┐
 *                    ▼                  ▼                  ▼
 *             [CustomDecoder]    [HttpServerCodec]    [MqttDecoder]
 *                    │                  │                  │
 *             [CustomHandler]    [HttpHandler]       [MqttHandler]
 * </pre>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NettyServerBootstrap {

    private final NettyServerProperties properties;

    /** Acceptor 线程组，负责接收新连接（1个线程足够） */
    private EventLoopGroup bossGroup;
    /** I/O 线程组，负责读写数据（默认 CPU核心数×2） */
    private EventLoopGroup workerGroup;
    /** 业务处理线程组，独立于 I/O 线程，避免阻塞事件循环 */
    private EventExecutorGroup businessGroup;
    /** 服务端监听 Channel，用于优雅关闭 */
    private Channel serverChannel;

    /**
     * Spring 容器初始化后启动 Netty 服务。
     *
     * <p>自动检测 Epoll 可用性：Linux 环境启用 Epoll 获得更佳性能，其他环境回退到 NIO。
     */
    @PostConstruct
    public void start() throws InterruptedException {
        boolean useEpoll = Epoll.isAvailable();
        log.info("Netty server starting, Epoll available: {}", useEpoll);

        // 根据平台选择 EventLoopGroup 实现
        bossGroup = useEpoll
                ? new EpollEventLoopGroup(properties.getBossThreads())
                : new NioEventLoopGroup(properties.getBossThreads());

        workerGroup = useEpoll
                ? new EpollEventLoopGroup(properties.getWorkerThreads())
                : new NioEventLoopGroup(properties.getWorkerThreads());

        businessGroup = new DefaultEventExecutorGroup(properties.getBusinessThreads());

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                // 内核参数：允许排队的连接数
                .option(ChannelOption.SO_BACKLOG, properties.getSoBacklog())
                // 开启 TCP 底层心跳
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                // 禁用 Nagle 算法，降低延迟
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();

                        // 1. 空闲状态检测：指定时间内无读数据则触发 IdleStateEvent
                        pipeline.addLast(new IdleStateHandler(
                                properties.getIdleTimeoutSeconds(), 0, 0, TimeUnit.SECONDS));

                        // 2. 多协议探测器：嗅探前几个字节判断协议类型，动态替换自身为对应编解码器
                        pipeline.addLast(new MultiProtocolDetector(properties));

                        // 3. 业务处理器：放入独立线程池执行，避免阻塞 I/O 线程
                        pipeline.addLast(businessGroup, "customHandler",
                                new CustomProtocolHandler(properties.getMaxIdleCount()));
                        pipeline.addLast(businessGroup, "httpHandler", new HttpBusinessHandler());
                        pipeline.addLast(businessGroup, "mqttHandler", new MqttBusinessHandler());
                    }
                });

        ChannelFuture future = bootstrap.bind(properties.getPort()).sync();
        serverChannel = future.channel();
        log.info("Netty server started on port: {} (Epoll: {})", properties.getPort(), useEpoll);
    }

    /**
     * Spring 容器关闭时优雅停机。
     *
     * <p>关闭顺序：先关闭服务端监听 Channel → 再依次关闭 bossGroup、workerGroup、businessGroup。
     * shutdownGracefully() 会等待已提交的任务完成后再关闭，避免丢失正在处理的请求。
     */
    @PreDestroy
    public void stop() {
        log.info("Netty server shutting down...");
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
        log.info("Netty server shut down complete");
    }
}
