package com.xsh.netty.serialize;

import java.io.IOException;

/**
 * 序列化接口，支持多种序列化方式的统一抽象。
 *
 * <p>当前已实现：JSON（type=1）
 * <p>预留：Protobuf（type=2）
 *
 * <p>通过 {@link #getSerializer(byte)} 工厂方法根据类型码获取对应实现。
 */
public interface Serializer {

    /** JSON 序列化类型码 */
    byte JSON_SERIALIZATION = 1;
    /** Protobuf 序列化类型码（预留） */
    byte PROTOBUF_SERIALIZATION = 2;

    /**
     * 将对象序列化为字节数组。
     */
    byte[] serialize(Object obj) throws IOException;

    /**
     * 将字节数组反序列化为指定类型的对象。
     */
    <T> T deserialize(Class<T> clazz, byte[] bytes) throws IOException;

    /**
     * 获取当前序列化器的类型码，对应协议帧头部的 serializationType 字段。
     */
    byte getType();

    /**
     * 根据序列化类型码获取对应的序列化器实例。
     *
     * @param type 序列化类型码（1=JSON）
     * @return 对应的序列化器实例
     * @throws IllegalArgumentException 不支持的序列化类型
     */
    static Serializer getSerializer(byte type) {
        if (type == JSON_SERIALIZATION) {
            return JsonSerializer.INSTANCE;
        }
        if (type == PROTOBUF_SERIALIZATION) {
            return ProtobufSerializer.INSTANCE;
        }
        throw new IllegalArgumentException("不支持的序列化类型: " + type);
    }
}
