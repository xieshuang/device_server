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

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Netty 测试客户端，用于验证自定义协议的编解码、心跳检测等功能。
 *
 * <p>支持的操作：
 * <ul>
 *   <li>自动发送心跳：连接建立后，每隔 idleTimeout 秒自动发送 PING</li>
 *   <li>手动发送业务数据：在控制台输入文本后回车发送</li>
 *   <li>输入 quit 退出客户端</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>
 * # 默认连接 127.0.0.1:9000
 * java com.xsh.netty.client.TestClient
 *
 * # 指定地址和端口
 * java com.xsh.netty.client.TestClient 192.168.1.100 9000
 * </pre>
 */
@Slf4j
public class TestClient {

    private static final int MAX_FRAME_LENGTH = 10485760; // 10MB
    private static final int IDLE_TIMEOUT_SECONDS = 5;

    private final String host;
    private final int port;

    public TestClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();

                            // 空闲检测：超时未收到服务端数据时触发事件，用于自动发送心跳
                            pipeline.addLast(new IdleStateHandler(0, IDLE_TIMEOUT_SECONDS, 0, TimeUnit.SECONDS));

                            // 编解码器
                            pipeline.addLast(new CustomProtocolEncoder());
                            pipeline.addLast(new CustomProtocolDecoder(MAX_FRAME_LENGTH));

                            // 业务处理
                            pipeline.addLast(new TestClientHandler());
                        }
                    });

            Channel channel = bootstrap.connect(host, port).sync().channel();
            log.info("已连接到服务端 {}:{}", host, port);

            // 控制台输入，发送业务数据
            Scanner scanner = new Scanner(System.in);
            while (channel.isActive()) {
                String line = scanner.nextLine();
                if ("quit".equalsIgnoreCase(line)) {
                    channel.close();
                    break;
                }
                if (line.isBlank()) {
                    continue;
                }
                sendBusiness(channel, line);
            }

            channel.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
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
        log.debug("发送心跳请求 → {}", channel.remoteAddress());
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
        log.info("发送业务数据 → {}: {}", channel.remoteAddress(), data);
    }

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "127.0.0.1";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 9000;
        new TestClient(host, port).start();
    }

    /**
     * 客户端业务处理器。
     *
     * <p>职责：
     * <ul>
     *   <li>接收服务端响应（心跳响应、业务响应）</li>
     *   <li>写空闲时自动发送心跳保活</li>
     * </ul>
     */
    @Slf4j
    static class TestClientHandler extends SimpleChannelInboundHandler<MessagePacket> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, MessagePacket packet) {
            MessageHeader header = packet.getHeader();
            switch (header.getMsgType()) {
                case MsgType.HEARTBEAT_RESP -> log.info("收到心跳响应 ← {}", ctx.channel().remoteAddress());
                case MsgType.BUSINESS -> log.info("收到业务响应 ← {}: {}", ctx.channel().remoteAddress(), packet.getBody());
                default -> log.warn("收到未知消息类型: {}", header.getMsgType());
            }
        }

        /**
         * 空闲事件回调：写空闲时自动发送心跳，保持连接存活。
         */
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
        public void channelActive(ChannelHandlerContext ctx) {
            log.info("连接建立: {}", ctx.channel().remoteAddress());
            // 连接建立后立即发送一次心跳
            sendHeartbeat(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.info("连接断开: {}", ctx.channel().remoteAddress());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("客户端异常", cause);
            ctx.close();
        }
    }
}
