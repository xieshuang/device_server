package com.xsh.netty.serialize;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Protobuf 序列化器单元测试。
 */
class ProtobufSerializerTest {

    @Test
    void testGetType() {
        assertEquals(Serializer.PROTOBUF_SERIALIZATION, ProtobufSerializer.INSTANCE.getType());
    }

    @Test
    void testSerialize_NonProtobufType_ThrowsException() {
        assertThrows(Exception.class, () ->
                ProtobufSerializer.INSTANCE.serialize("not a protobuf object"));
    }

    @Test
    void testDeserialize_UnregisteredType_ThrowsException() {
        assertThrows(Exception.class, () ->
                ProtobufSerializer.INSTANCE.deserialize(String.class, new byte[]{0x01}));
    }

    @Test
    void testGetSerializer_ProtoType() {
        Serializer s = Serializer.getSerializer(Serializer.PROTOBUF_SERIALIZATION);
        assertNotNull(s);
        assertEquals(Serializer.PROTOBUF_SERIALIZATION, s.getType());
    }

    @Test
    void testGetSerializer_JsonType() {
        Serializer s = Serializer.getSerializer(Serializer.JSON_SERIALIZATION);
        assertNotNull(s);
        assertEquals(Serializer.JSON_SERIALIZATION, s.getType());
    }
}
