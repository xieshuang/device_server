package com.xsh.netty.client;

import com.xsh.netty.auth.HmacUtils;
import com.xsh.netty.codec.CustomProtocolDecoder;
import com.xsh.netty.codec.CustomProtocolEncoder;
import com.xsh.netty.protocol.AuthRequest;
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

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Netty 测试客户端，支持鉴权、心跳保活、断线重连。
 *
 * <p>支持的操作：
 * <ul>
 *   <li>连接建立后自动发送鉴权请求</li>
 *   <li>鉴权通过后自动发送心跳保活</li>
 *   <li>手动发送业务数据：在控制台输入文本后回车发送</li>
 *   <li>断线后自动重连（指数退避：1s→2s→4s→8s→16s→30s）</li>
 *   <li>输入 quit 退出客户端</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>
 * # 默认连接 127.0.0.1:9000，使用测试设备ID
 * java com.xsh.netty.client.TestClient
 *
 * # 指定地址、端口、设备ID和密钥
 * java com.xsh.netty.client.TestClient 192.168.1.100 9000 test-device-001 my-secret-key
 * </pre>
 */
@Slf4j
public class TestClient {

    private static final int MAX_FRAME_LENGTH = 10485760; // 10MB
    private static final int IDLE_TIMEOUT_SECONDS = 5;
    /** 最大重连间隔（秒） */
    private static final int MAX_RECONNECT_DELAY_SECONDS = 30;

    private final String host;
    private final int port;
    private final String deviceId;
    private final String productSecret;

    /** 当前重连延迟（秒），指数退避 */
    private int reconnectDelay = 1;
    /** 是否主动退出 */
    private volatile boolean shutdown = false;
    /** 共享 EventLoopGroup */
    private EventLoopGroup group;
    /** 当前 Channel */
    private volatile Channel currentChannel;

    public TestClient(String host, int port, String deviceId, String productSecret) {
        this.host = host;
        this.port = port;
        this.deviceId = deviceId;
        this.productSecret = productSecret;
    }

    public void start() throws InterruptedException {
        group = new NioEventLoopGroup();
        try {
            connect();

            // 控制台输入，发送业务数据
            Scanner scanner = new Scanner(System.in);
            while (!shutdown) {
                String line = scanner.nextLine();
                if ("quit".equalsIgnoreCase(line)) {
                    shutdown = true;
                    if (currentChannel != null) {
                        currentChannel.close();
                    }
                    break;
                }
                if (line.isBlank() || currentChannel == null || !currentChannel.isActive()) {
                    continue;
                }
                sendBusiness(currentChannel, line);
            }

            if (currentChannel != null) {
                currentChannel.closeFuture().sync();
            }
        } finally {
            group.shutdownGracefully();
        }
    }

    /**
     * 建立连接，失败时触发重连。
     */
    private void connect() {
        if (shutdown) return;

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(0, IDLE_TIMEOUT_SECONDS, 0, TimeUnit.SECONDS));
                        pipeline.addLast(new CustomProtocolEncoder());
                        pipeline.addLast(new CustomProtocolDecoder(MAX_FRAME_LENGTH));
                        pipeline.addLast(new TestClientHandler());
                    }
                });

        bootstrap.connect(host, port).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                currentChannel = future.channel();
                reconnectDelay = 1; // 重置退避
                log.info("已连接到服务端 {}:{}", host, port);
            } else {
                log.warn("连接失败: {}，{}秒后重连", future.cause().getMessage(), reconnectDelay);
                scheduleReconnect();
            }
        });
    }

    /**
     * 指数退避重连。
     */
    private void scheduleReconnect() {
        if (shutdown) return;
        group.schedule(() -> {
            reconnectDelay = Math.min(reconnectDelay * 2, MAX_RECONNECT_DELAY_SECONDS);
            connect();
        }, reconnectDelay, TimeUnit.SECONDS);
    }

    /**
     * 发送鉴权请求。
     */
    private void sendAuth(Channel channel) {
        long timestamp = System.currentTimeMillis();
        String token = HmacUtils.computeToken(deviceId, timestamp, productSecret);
        log.info("client token = {}", token);
        AuthRequest authReq = new AuthRequest();
        authReq.setDeviceId(deviceId);
        authReq.setTimestamp(timestamp);
        authReq.setToken(token);

        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.AUTH_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody(authReq);
        channel.writeAndFlush(packet);
        log.info("发送鉴权请求: deviceId={}", deviceId);
    }

    /**
     * 发送心跳请求。
     */
    private static void sendHeartbeat(Channel channel) {
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.HEARTBEAT_REQ);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody("PING");
        channel.writeAndFlush(packet);
    }

    /**
     * 发送业务数据。
     */
    private static void sendBusiness(Channel channel, String data) {
        MessagePacket packet = new MessagePacket();
        MessageHeader header = new MessageHeader();
        header.setMsgType(MsgType.BUSINESS);
        header.setSerializationType(Serializer.JSON_SERIALIZATION);
        packet.setHeader(header);
        packet.setBody(data);
        channel.writeAndFlush(packet);
        log.info("发送业务数据: {}", data);
    }

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;
        String deviceId = args.length > 2 ? args[2] : "test-device-001";
        String productSecret = args.length > 3 ? args[3] : "test-secret-key";
        new TestClient(host, port, deviceId, productSecret).start();
    }

    /**
     * 客户端业务处理器。
     *
     * <p>职责：
     * <ul>
     *   <li>连接建立后自动发送鉴权请求</li>
     *   <li>鉴权通过后自动发送心跳保活</li>
     *   <li>断线后触发重连</li>
     * </ul>
     */
    class TestClientHandler extends SimpleChannelInboundHandler<MessagePacket> {

        private boolean authenticated = false;

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("连接建立: {}", ctx.channel().remoteAddress());
            // 连接建立后立即发送鉴权请求
            sendAuth(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("连接断开: {}", ctx.channel().remoteAddress());
            authenticated = false;
            // 非主动退出时触发重连
            if (!shutdown) {
                scheduleReconnect();
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) {
            MessageHeader header = packet.getHeader();
            switch (header.getMsgType()) {
                case MsgType.AUTH_RESP -> {
                    authenticated = true;
                    log.info("鉴权成功: deviceId={}", deviceId);
                    // 鉴权成功后发送首次心跳
                    sendHeartbeat(ctx.channel());
                }
                case MsgType.AUTH_FAIL -> {
                    log.error("鉴权失败: {}", packet.getBody());
                    ctx.close();
                }
                case MsgType.HEARTBEAT_RESP -> log.debug("心跳响应 ← {}", ctx.channel().remoteAddress());
                case MsgType.BUSINESS -> log.info("业务响应 ← {}: {}", ctx.channel().remoteAddress(), packet.getBody());
                default -> log.warn("未知消息类型: {}", header.getMsgType());
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent idleEvent) {
                if (idleEvent.state() == IdleState.WRITER_IDLE && authenticated) {
                    sendHeartbeat(ctx.channel());
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("客户端异常", cause);
            ctx.close();
        }
    }
}
