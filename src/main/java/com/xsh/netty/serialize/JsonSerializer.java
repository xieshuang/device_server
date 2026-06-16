package com.xsh.netty.serialize;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * JSON 序列化器实现，基于 Jackson ObjectMapper。
 *
 * <p>使用单例模式（{@link #INSTANCE}），避免重复创建 ObjectMapper 实例。
 * ObjectMapper 是线程安全的，可以在多线程环境中共享使用。
 */
public class JsonSerializer implements Serializer {

    /** 单例实例 */
    public static final JsonSerializer INSTANCE = new JsonSerializer();

    /** Jackson ObjectMapper，线程安全，全局共享 */
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Object obj) throws IOException {
        return objectMapper.writeValueAsBytes(obj);
    }

    @Override
    public <T> T deserialize(Class<T> clazz, byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, clazz);
    }

    @Override
    public byte getType() {
        return JSON_SERIALIZATION;
    }
}
