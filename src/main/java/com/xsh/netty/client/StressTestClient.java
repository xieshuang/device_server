package com.xsh.netty.client;

import com.xsh.netty.codec.CustomProtocolDecoder;
import com.xsh.netty.codec.CustomProtocolEncoder;
import com.xsh.netty.protocol.MessageHeader;
import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import com.xsh.netty.serialize.Serializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 压力测试客户端，模拟大量并发连接。
 *
 * <p>核心设计：
 * <ul>
 *   <li>所有连接共享同一个 EventLoopGroup（CPU核心数×2线程），避免万级连接创建万个线程</li>
 *   <li>逐步递增连接（ramp-up），避免瞬间冲击导致服务端拒绝连接</li>
 *   <li>实时统计：连接成功/失败数、已断开数、心跳收发数</li>
 *   <li>每5秒写空闲自动发送心跳保活，可配置周期发送业务数据</li>
 *   <li>使用 ConcurrentHashMap 管理所有活跃 Channel，支持向全部连接群发业务数据</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>
 * # 默认：连接 127.0.0.1:9000，10000个连接，每批100个，批次间隔50ms
 * java com.xsh.netty.client.StressTestClient
 *
 * # 指定参数：主机 端口 连接数 每批大小 批次间隔(ms) 业务发送间隔(秒,0=不发)
 * java com.xsh.netty.client.StressTestClient 192.168.1.100 9000 10000 100 50 30
 * </pre>
 *
 * <p>运行前注意：
 * <ul>
 *   <li>Linux 需调整文件描述符上限：ulimit -n 65535</li>
 *   <li>Windows 默认临时端口约16000个，万级连接需调整注册表 MaxUserPort</li>
 *   <li>服务端需确保 SO_BACKLOG 足够大</li>
 * </ul>
 */
@Slf4j
public class StressTestClient {

    private static final int MAX_FRAME_LENGTH = 10485760; // 10MB
    private static final int IDLE_TIMEOUT_SECONDS = 5;

    /** 统计：连接成功数 */
    private final AtomicInteger successCount = new AtomicInteger(0);
    /** 统计：连接失败数 */
    private final AtomicInteger failCount = new AtomicInteger(0);
    /** 统计：已断开连接数 */
    private final AtomicInteger disconnectCount = new AtomicInteger(0);
    /** 统计：收到的心跳响应数 */
    private final AtomicLong heartbeatRespCount = new AtomicLong(0);
    /** 统计：发出的心跳请求数 */
    private final AtomicLong heartbeatReqCount = new AtomicLong(0);
    /** 统计：发出的业务消息数 */
    private final AtomicLong businessReqCount = new AtomicLong(0);

    /** 所有活跃连接的 Channel 集合，用于群发业务数据 */
    private final ConcurrentHashMap<String, Channel> activeChannels = new ConcurrentHashMap<>();

    private final String host;
    private final int port;
    private final int totalConnections;
    private final int batchSize;
    private final int batchIntervalMs;
    private final int businessIntervalSeconds;

    public StressTestClient(String host, int port, int totalConnections,
                            int batchSize, int batchIntervalMs,
                            int businessIntervalSeconds) {
        this.host = host;
        this.port = port;
        this.totalConnections = totalConnections;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.businessIntervalSeconds = businessIntervalSeconds;
    }

    public void start() throws Exception {
        log.info("========== 压力测试启动 ==========");
        log.info("目标: {}:{}, 总连接数: {}, 每批: {}, 批次间隔: {}ms, 业务发送间隔: {}s",
                host, port, totalConnections, batchSize, batchIntervalMs, businessIntervalSeconds);

        // 共享 EventLoopGroup，万级连接只需少量线程
        EventLoopGroup group = new NioEventLoopGroup(
                Runtime.getRuntime().availableProcessors() * 2);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(
                                0, IDLE_TIMEOUT_SECONDS, 0, TimeUnit.SECONDS));
                        pipeline.addLast(new CustomProtocolEncoder());
                        pipeline.addLast(new CustomProtocolDecoder(MAX_FRAME_LENGTH));
                        pipeline.addLast(new StressClientHandler(
                                heartbeatReqCount, heartbeatRespCount,
                                disconnectCount, activeChannels));
                    }
                });

        // 启动统计报告线程：每10秒打印一次
        ScheduledExecutorService statsScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "stats-reporter"));
        statsScheduler.scheduleAtFixedRate(this::printStats, 5, 10, TimeUnit.SECONDS);

        // 启动业务数据发送线程：按配置周期向所有连接群发
        ScheduledExecutorService businessScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "business-sender"));
        if (businessIntervalSeconds > 0) {
            businessScheduler.scheduleAtFixedRate(
                    this::sendBusinessToAll, 10, businessIntervalSeconds, TimeUnit.SECONDS);
        }

        // 逐步建立连接
        long startTime = System.currentTimeMillis();
        int connected = 0;

        while (connected < totalConnections) {
            int count = Math.min(batchSize, totalConnections - connected);
            CountDownLatch batchLatch = new CountDownLatch(count);

            for (int i = 0; i < count; i++) {
                final int index = connected + i + 1;
                bootstrap.connect(host, port).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        Channel channel = future.channel();
                        activeChannels.put(channel.id().asShortText(), channel);
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                        log.debug("连接 #{} 失败: {}", index, future.cause().getMessage());
                    }
                    batchLatch.countDown();
                });
            }

            batchLatch.await(30, TimeUnit.SECONDS);
            connected += count;

            // 批次间等待，避免瞬间冲击
            if (connected < totalConnections && batchIntervalMs > 0) {
                Thread.sleep(batchIntervalMs);
            }

            // 每1000个连接打印一次进度
            if (connected % 1000 == 0) {
                log.info("连接进度: {}/{}", connected, totalConnections);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("连接建立完成，耗时: {}ms，成功: {}, 失败: {}",
                elapsed, successCount.get(), failCount.get());

        // 等待用户输入退出
        log.info("所有连接已建立，按 Enter 键退出...");
        System.in.read();

        // 关闭
        businessScheduler.shutdownNow();
        statsScheduler.shutdownNow();
        group.shutdownGracefully();

        printStats();
        log.info("========== 压力测试结束 ==========");
    }

    /**
     * 打印实时统计信息。
     */
    private void printStats() {
        log.info("[统计] 活跃: {}, 成功: {}, 失败: {}, 断开: {}, 心跳发送: {}, 心跳响应: {}, 业务发送: {}",
                activeChannels.size(), successCount.get(), failCount.get(), disconnectCount.get(),
                heartbeatReqCount.get(), heartbeatRespCount.get(), businessReqCount.get());
    }

    /**
     * 向所有活跃连接发送业务数据。
     * 遍历 Channel 集合，逐个发送，自动跳过已断开的连接。
     */
    private void sendBusinessToAll() {
        int sent = 0;
        var iterator = activeChannels.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            Channel channel = entry.getValue();
            if (channel.isActive()) {
                channel.writeAndFlush(buildBusinessPacket());
                sent++;
            } else {
                // 清理已断开的 Channel
                iterator.remove();
            }
        }
        businessReqCount.addAndGet(sent);
    }

    /**
     * 构建业务数据包。
     */
    private static MessagePacket buildBusinessPacket() {
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody("stress-test-data");
        return packet;
    }

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;
        int totalConnections = args.length > 2 ? Integer.parseInt(args[2]) : 10000;
        int batchSize = args.length > 3 ? Integer.parseInt(args[3]) : 100;
        int batchIntervalMs = args.length > 4 ? Integer.parseInt(args[4]) : 50;
        int businessIntervalSeconds = args.length > 5 ? Integer.parseInt(args[5]) : 0;

        new StressTestClient(host, port, totalConnections,
                batchSize, batchIntervalMs, businessIntervalSeconds).start();
    }

    /**
     * 压测客户端业务处理器。
     *
     * <p>职责：
     * <ul>
     *   <li>连接建立后发送首次心跳</li>
     *   <li>写空闲时自动发送心跳保活</li>
     *   <li>收到响应时更新统计计数器</li>
     *   <li>连接断开时从 Channel 集合中移除并更新统计</li>
     * </ul>
     *
     * <p>压测场景下不逐条打印日志，避免日志风暴影响性能。
     */
    @Slf4j
    static class StressClientHandler extends SimpleChannelInboundHandler<MessagePacket> {

        private final AtomicLong heartbeatReqCounter;
        private final AtomicLong heartbeatRespCounter;
        private final AtomicInteger disconnectCounter;
        private final ConcurrentHashMap<String, Channel> activeChannels;

        StressClientHandler(AtomicLong heartbeatReqCounter, AtomicLong heartbeatRespCounter,
                            AtomicInteger disconnectCounter,
                            ConcurrentHashMap<String, Channel> activeChannels) {
            this.heartbeatReqCounter = heartbeatReqCounter;
            this.heartbeatRespCounter = heartbeatRespCounter;
            this.disconnectCounter = disconnectCounter;
            this.activeChannels = activeChannels;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            // 连接建立后立即发送首次心跳
            sendHeartbeat(ctx.channel());
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) {
            MessageHeader header = packet.getHeader();
            if (header.getMsgType() == MsgType.HEARTBEAT_RESP) {
                heartbeatRespCounter.incrementAndGet();
            }
            // 压测场景下不逐条打印业务响应，避免日志风暴
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent idleEvent) {
                if (idleEvent.state() == IdleState.WRITER_IDLE) {
                    sendHeartbeat(ctx.channel());
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            // 从活跃集合中移除，更新断开计数
            activeChannels.remove(ctx.channel().id().asShortText());
            disconnectCounter.incrementAndGet();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        private void sendHeartbeat(Channel channel) {
            if (!channel.isActive()) return;
            MessagePacket packet = new MessagePacket();
            MessageHeader header = new MessageHeader();
            header.setMsgType(MsgType.HEARTBEAT_REQ);
            header.setSerializationType(Serializer.JSON_SERIALIZATION);
            packet.setHeader(header);
            packet.setBody("PING");
            channel.writeAndFlush(packet);
            heartbeatReqCounter.incrementAndGet();
        }
    }
}
