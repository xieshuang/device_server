package com.xsh.netty;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 设备服务主启动类。
 *
 * <p>基于 Spring Boot 3 + Netty 构建的工业级多协议设备接入服务器，
 * 支持自定义协议、HTTP、MQTT 三种协议接入，内置心跳检测机制。
 */
@SpringBootApplication
public class DeviceServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeviceServerApplication.class, args);
    }
}
