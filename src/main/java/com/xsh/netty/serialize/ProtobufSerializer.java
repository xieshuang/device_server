package com.xsh.netty.serialize;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Protobuf 序列化器实现，对应协议帧头部 serializationType=2。
 *
 * <p>设计要点：
 * <ul>
 *   <li>支持多消息类型：通过 Class → Parser 映射自动选择解析器</li>
 *   <li>线程安全：Parser 是不可变的，ConcurrentHashMap 保证并发注册安全</li>
 *   <li>运行时注册：可通过 {@link #registerParser} 动态添加新的 Protobuf 类型</li>
 * </ul>
 *
 * <p>使用方式：
 * <pre>
 * // 序列化
 * byte[] bytes = ProtobufSerializer.INSTANCE.serialize(businessMessage);
 *
 * // 反序列化
 * BusinessMessage msg = ProtobufSerializer.INSTANCE.deserialize(BusinessMessage.class, bytes);
 * </pre>
 */
@Slf4j
public class ProtobufSerializer implements Serializer {

    /** 全局单例 */
    public static final ProtobufSerializer INSTANCE = new ProtobufSerializer();

    /** Class → Parser 映射，运行时动态注册 */
    private final ConcurrentHashMap<Class<?>, Parser<?>> parserMap = new ConcurrentHashMap<>();

    private ProtobufSerializer() {
        // 注册内置 Protobuf 消息类型的 Parser
        // 注意：这些类由 protobuf-maven-plugin 从 .proto 文件编译生成
        try {
            Class<?> businessMsgClass = Class.forName(
                    "com.xsh.netty.protocol.protobuf.DeviceMessageProto$BusinessMessage");
            registerParserFromClass(businessMsgClass);
        } catch (ClassNotFoundException e) {
            log.info("BusinessMessage Protobuf 类未找到（可能未编译 proto），跳过注册");
        }

        try {
            Class<?> authReqClass = Class.forName(
                    "com.xsh.netty.protocol.protobuf.DeviceMessageProto$AuthRequestProto");
            registerParserFromClass(authReqClass);
        } catch (ClassNotFoundException e) {
            log.info("AuthRequestProto Protobuf 类未找到（可能未编译 proto），跳过注册");
        }
    }

    /**
     * 通过反射从 Protobuf 生成类中提取 Parser 并注册。
     */
    @SuppressWarnings("unchecked")
    private void registerParserFromClass(Class<?> clazz) {
        try {
            Object defaultInstance = clazz.getMethod("getDefaultInstance").invoke(null);
            Parser<?> parser = (Parser<?>) clazz.getMethod("getParserForType").invoke(defaultInstance);
            parserMap.put(clazz, parser);
            log.debug("Protobuf Parser 已注册: {}", clazz.getSimpleName());
        } catch (Exception e) {
            log.warn("Protobuf Parser 注册失败: {}", clazz.getSimpleName(), e);
        }
    }

    @Override
    public byte[] serialize(Object obj) throws IOException {
        if (obj instanceof MessageLite message) {
            return message.toByteArray();
        }
        throw new IOException("不支持的序列化类型: " + obj.getClass().getName()
                + "，Protobuf 序列化仅支持 MessageLite 子类");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(Class<T> clazz, byte[] bytes) throws IOException {
        Parser<?> parser = parserMap.get(clazz);
        if (parser != null) {
            return (T) parser.parseFrom(bytes);
        }
        throw new IOException("未注册的 Protobuf 类型: " + clazz.getName()
                + "，请先调用 registerParser() 注册");
    }

    @Override
    public byte getType() {
        return PROTOBUF_SERIALIZATION;
    }

    /**
     * 注册自定义 Protobuf 消息类型的 Parser。
     *
     * @param clazz 消息类型
     * @param parser 对应的 Parser 实例
     */
    public void registerParser(Class<?> clazz, Parser<?> parser) {
        parserMap.put(clazz, parser);
        log.info("Protobuf Parser 已注册: {}", clazz.getSimpleName());
    }
}
