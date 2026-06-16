package com.xsh.netty.config;

import com.xsh.netty.dispatcher.MessageDispatcher;
import com.xsh.netty.dispatcher.MessageHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * 消息分发器配置，自动收集所有 MessageHandler 实现并注册到 Dispatcher。
 */
@Configuration
public class DispatcherConfig {

    @Bean
    public MessageDispatcher messageDispatcher(List<MessageHandler> handlers) {
        MessageDispatcher dispatcher = new MessageDispatcher();
        dispatcher.registerAll(handlers);
        return dispatcher;
    }
}
