# 自动化测试覆盖率报告

> 生成时间: 2026-06-17 18:01:10 | 等级: 🟠 C | 行覆盖: **54.3%**

## 总体

| 指标 | 值 |
|------|----|
| 总行数 | 1441 |
| 已覆盖 | 782 |
| 未覆盖 | 659 |
| ≥80%类 | 31 个 |
| 零覆盖类 | 15 个 |

## 包级

| 包 | 覆盖 | 率 | 图 |
|----|------|----|----|
| auth | 52 | 53% |  |
| codec | 187 | 79% |  |
| config | 53 | 29% |  |
| controller | 67 | 100% |  |
| dispatcher | 15 | 37% |  |
| handler | 179 | 43% |  |
| kafka | 4 | 9% |  |
| protocol | 28 | 97% |  |
| ratelimit | 50 | 100% |  |
| serialize | 33 | 73% |  |
| server | 113 | 50% |  |
| netty | 1 | 33% |  |

## ≥80% 类
| 类 | 覆盖 | 率 |
|----|------|----|
| MessageDispatcher | 15/15 | 100% |
| ClusterSessionManager | 17/17 | 100% |
| ModbusEncoder | 10/10 | 100% |
| ModbusFrame | 5/5 | 100% |
| ModbusDecoder | 32/32 | 100% |
| MessagePacket | 5/5 | 100% |
| VersionNegotiator | 8/8 | 100% |
| AuthRequest | 5/5 | 100% |
| MsgType | 1/1 | 100% |
| ChannelAttributes | 3/3 | 100% |
| FirewallController | 15/15 | 100% |
| RateLimitController | 12/12 | 100% |
| DeviceManagementController | 40/40 | 100% |
| NettyMetricsBinder | 53/53 | 100% |
| RateLimitHandler | 21/21 | 100% |
| RateLimiterService | 29/29 | 100% |
| KafkaMessageEnvelopeBuilder | 1/1 | 100% |
| DeviceRevocationService | 23/23 | 100% |
| NonceValidator | 17/17 | 100% |
| JsonSerializer | 6/6 | 100% |
| NettyServerProperties | 33/35 | 94% |
| CustomProtocolDecoder | 60/64 | 94% |
| CustomProtocolEncoder | 15/16 | 94% |
| CustomProtocolHandler | 55/62 | 89% |
| PendingAckManager | 30/34 | 88% |
| BusinessMessageHandler | 34/39 | 87% |
| MessageHeader | 6/7 | 86% |
| HmacUtils | 12/14 | 86% |
| MultiProtocolDetector | 65/79 | 82% |
| AuthHandler | 82/102 | 80% |
| Serializer | 4/5 | 80% |

## 零覆盖类
| 类 | 行数 |
|----|------|
| ThingModelMessageHandler | 7 |
| ThingModelContext | 19 |
| HttpBusinessHandler | 7 |
| WebSocketBusinessHandler | 101 |
| MqttBusinessHandler | 8 |
| IpFilterHandler | 12 |
| ClusterRouterService | 33 |
| WebSocketFrameCodec | 32 |
| DispatcherConfig | 4 |
| } | 22 |
| NettyServerBootstrap | 83 |
| HandlerBeanContainer | 19 |
| KafkaProducerConfig | 12 |
| AuthService | 1 |
| RedisAuthService | 43 |
