package com.xsh.netty.dispatcher;

import com.xsh.netty.protocol.MessagePacket;
import com.xsh.netty.protocol.MsgType;
import io.netty.channel.ChannelHandlerContext;

import java.nio.charset.StandardCharsets;

/**
 * 物模型转换处理器接口，将异构设备原始数据转换为标准物模型 JSON。
 *
 * <p>实现类通过 Spring Bean 注册，由 {@link DispatcherConfig} 自动装配到
 * {@link MessageDispatcher} 中，零侵入核心代码。
 *
 * <p>使用方式（业务方）：
 * <pre>
 * &#64;Component
 * public class MyDeviceThingModel implements ThingModelMessageHandler {
 *     &#64;Override
 *     public String convertToThingModel(ThingModelContext ctx) {
 *         byte[] raw = ctx.getRawBytes();
 *         // 将原始 Hex/二进制数据解析为标准物模型 JSON
 *         return buildStandardJson(ctx.getDeviceId(), raw);
 *     }
 * }
 * </pre>
 *
 * <p>V5 预留：后续可通过 {@code ScriptEngineMessageHandler} 支持从配置中心
 * 动态加载 JavaScript/GraalVM 脚本实现协议转换，无需重新部署。
 */
public interface ThingModelMessageHandler extends MessageHandler {

    /**
     * 将设备原始数据转换为标准物模型 JSON 字符串。
     *
     * @param ctx 物模型上下文（deviceId / rawBytes / serializationType / 协议标识）
     * @return 标准物模型 JSON 字符串；null 表示无需转换（透传原始数据）
     */
    String convertToThingModel(ThingModelContext ctx);

    @Override
    default byte supportMsgType() {
        // 接管所有��务报文，在 BusinessMessageHandler 之前执行转换
        return MsgType.BUSINESS;
    }

    @Override
    default void handle(ChannelHandlerContext ctx, String deviceId, MessagePacket packet) {
        ThingModelContext tmCtx = new ThingModelContext(deviceId, packet);
        String standardJson = convertToThingModel(tmCtx);
        if (standardJson != null) {
            // 将转换后的物模型 JSON 回填到消息包，继续沿 Pipeline 流转
            packet.setBody(standardJson);
            packet.setRawBody(standardJson.getBytes(StandardCharsets.UTF_8));
        }
        // 注意：此 Handler 不调用 ctx.fireChannelRead()，因为后续
        // BusinessMessageHandler 已在 Pipeline 中通过 Dispatcher 串联调用。
        // 此处仅做数据转换，实际 Kafka 投递由 BusinessMessageHandler.handle() 完成。
    }
}
