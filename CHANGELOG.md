# Changelog

All notable changes to the Industrial IoT Device Server.

Format based on [Keep a Changelog](https://keepachangelog.com/).

---

## [5.3.0] - 2026-06-17

### Added
- Prometheus 11 条告警规则（鉴权失败率/ACK 积压/Kafka 发送/连接数/限流/JVM 内存）
- Jasypt 配置加密使用指南（`docs/config-encryption-guide.md`）
- 压力测试基线脚本 `load-test-baseline.sh`（1K/5K/10K 三级内存基线）

---

## [5.2.0] - 2026-06-17

### Added
- MQTT 3.1.1 完整协议：CONNECT/CONNACK 握手、PUBLISH/PUBACK（QoS 0/1）、SUBSCRIBE/SUBACK、UNSUBSCRIBE/UNSUBACK、PINGREQ/PINGRESP、DISCONNECT
- `MqttTopicManager`：Topic 树 + `+`/`#` 通配符匹配 + 跨连接消息广播
- `ScriptEngineMessageHandler`：Redis 脚本热加载 + 超时沙箱执行（5s）+ Nashorn 降级
- 物模型 V2 动态脚本引擎（`@ConditionalOnProperty` 条件装配）

### Changed
- `MqttBusinessHandler`：从 TODO 存根重构为完整消息处理状态机
- `MultiProtocolDetector`：MQTT Pipeline 按需动态插入 Handler
- `NettyServerBootstrap`：移除静态 MqttBusinessHandler 避免污染所有连接

---

## [5.1.0] - 2026-06-17

### Added
- 运维 REST API（3 个 Controller，9 个端点）：
  - `DeviceManagementController`：在线列表/强制下线/设备吊销/统计
  - `FirewallController`：IP 封禁查询/手动封禁
  - `RateLimitController`：全局/单设备限流动态调整
- P0 Handler 测试：`CustomProtocolHandlerTest`（9 用例）、`NettyMetricsBinderTest`（10 用例）
- 安全与集群测试：`NonceValidatorTest`（5）、`DeviceRevocationServiceTest`（6）、`RateLimitHandlerTest`（5）、`ClusterSessionManagerTest`（5）
- API 集成测试：`DeviceManagementControllerTest`（8）、`FirewallControllerTest`（3）、`RateLimitControllerTest`（2）

### Changed
- 测试用例 75 → 130（增长 73%）
- 行覆盖率 31.7% → 54.3%

---

## [5.0.0] - 2026-06-17

### Added
- **安全增强**：
  - `NonceValidator`：Redis SETNX 原子 nonce 防重放
  - `DeviceRevocationService`：设备吊销/解除/查询（Redis 持久化）
- **全链路追踪**：`MessagePacket.traceId` 贯穿解码→鉴权→Dispatcher→Kafka
- **配置加密**：Jasypt Spring Boot Starter 3.0.5
- **测试补齐**（75 用例，14 测试类）：
  - 编解码测试：`CustomProtocolCodecTest`、`ModbusCodecTest`、`MultiProtocolDetectorTest`
  - 业务逻辑测试：`HmacUtilsTest`、`VersionNegotiatorTest`、`PendingAckManagerTest`、`DeviceChannelManagerTest`、`MessageDispatcherTest`、`BackpressureHandlerTest`、`RateLimiterServiceTest`、`BusinessMessageHandlerTest`、`AuthHandlerTest`、`ProtobufSerializerTest`
- JaCoCo 0.8.11 覆盖率插件
- 自动化测试报告脚本 `test-report.sh`

### Changed
- `AuthRequest`：新增 `nonce` 字段（null 兼容旧客户端）
- `AuthService`：新增 4 参数 `authenticate(deviceId, timestamp, token, nonce)`
- `RedisAuthService`：鉴权流程增加 nonce 校验 + 吊销检查
- `BusinessMessageHandler`：Kafka Header 增加 `traceId`
- `CustomProtocolDecoder`：解码时生成 12 位 UUID traceId
- 测试用例 6 → 75（增长 12.5 倍）

---

## [4.1.0] - 2026-06-17

### Added
- **Modbus-TCP** 工业协议：
  - `ModbusDecoder`/`ModbusEncoder`：MBAP 头解析 + PDU 编解码
  - `ModbusFrame`：数据帧 POJO
  - `ModbusBusinessHandler`：6 种功能码（01/02/03/04/06/10）
  - MBAP 增强嗅探（b2/b3=0x0000 + b4=0x00 防误判）
- **OPC-UA** 工业协议：
  - `OpcUaBusinessHandler`：HEL→ACK/OPN/MSG/CLO 状态机
  - 6 种消息类型识别（HEL/ACK/OPN/MSG/CLO/ERR）
  - Eclipse Milo SDK 0.6.13
- **物模型插件**：
  - `ThingModelMessageHandler`：零侵入接口（extends MessageHandler）
  - `ThingModelContext`：物模型上下文
  - `DispatcherConfig` 自动装配

### Changed
- `MultiProtocolDetector`：协议嗅探优先级 5→6（DVSR/Modbus/OPC-UA/HTTP/WS/MQTT）

---

## [4.0.0] - 2026-06-17

### Added
- **HashedWheelTimer ACK 优化**：双 ConcurrentHashMap + LongAdder 替代 Caffeine，2048 槽位时间轮
- **TCP 背压流控**：`BackpressureHandler`，基于 `channelWritabilityChanged` 双向驱动
- **IP 动态黑名单**：`IpFirewallService`（Redis + Caffeine）+ `IpFilterHandler`
- **优雅停机**：广播维护通知 + Kafka `flushBuffer()` + `shutdownGracefully` 超时隔离
- **分布式集群组件**：
  - `ClusterSessionManager`：Lua 原子令牌校验注销
  - `ClusterRouterService`：Redis Pub/Sub 跨节点指令路由
  - `@PostConstruct` node-id 空值强防御校验
- 13 个 V4 新增配置项

### Changed
- `PendingAckManager`：Caffeine TTL → HashedWheelTimer
- `NettyServerBootstrap.stop()`：四阶段优雅停机流程
- `DeviceChannelManager`：增加 `broadcastMaintenanceNotice()`/`closeAll()`
- `KafkaProducerService`：增加 `flushBuffer()` 替代 `CompletableFuture.join()`
- `HandlerBeanContainer`：增加 `IpFirewallService` 依赖

---

## [3.1.0] - 2026-06-17

### Fixed
- **P0 安全漏洞**：`RedisAuthService` 删除 token 明文日志
- **P0 功能补全**：`WebSocketBusinessHandler` 完整鉴权实现
- **P0 条件装配**：`KafkaProducerConfig`/`KafkaProducerService` 增加 `@ConditionalOnProperty`
- **P1 健壮性**：`CustomProtocolHandler` BUSINESS 分支 try-catch
- **P1 资源隔离**：`NettyServerBootstrap.stop()` 独立 try-catch
- **P1 日志优化**：协议检测/版本协商 info→debug
- **P2 序列化**：`BusinessMessageHandler` 按 serializationType 区分 Protobuf/JSON
- **P2 版本协商**：NumberFormatException 不再默认 V2

### Changed
- Guava 降级到 32.1.3-jre（与 Spring Boot 3.2.5 兼容）

---

## [3.0.0] - 2026-06-17

### Added
- **Kafka 消息持久化**：`BusinessMessageHandler` → 统一 Topic `device-messages`，deviceId 分区保序
- **流量控制**：双维度令牌桶（Guava RateLimiter），心跳不限流
- **WebSocket 支持**：复用 HTTP 端口升级，`WebSocketFrameCodec` 帧转换
- **Grafana 看板**：12 块面板（连接/心跳/业务/鉴权/JVM 等指标）
- **Protobuf 序列化**：`serializationType=2` 自动路由，`.proto` 编译生成 Java 类
- **协议版本协商**：VERSION_NEGOTIATE(msgType=8) 动态协商
- **HandlerBeanContainer**：解决 Netty Handler 无法注入 Spring Bean
- **Kafka 条件装配**：`kafka-enabled=false` 时 Kafka Bean 不创建
- `pom.xml`：新增 Guava、Protobuf-java、protobuf-maven-plugin

### Fixed
- `CustomProtocolHandler` BUSINESS 消息接入 `MessageDispatcher`
- ACK 处理接入 `PendingAckManager.ack()`
- `NettyMetricsBinder` 指标接入各 Handler
- 业务消息反序列化改为 byte[] 透传

---

## [2.0.0] - 2026-06-14

### Added
- **设备鉴权**：HMAC-MD5 + Redis(Lettuce 异步) + AuthHandler（鉴权后自动移除）
- **连接管理**：`DeviceChannelManager`（踢旧保新、定向推送、在线统计）
- **消息确认**：`PendingAckManager`（Caffeine TTL 30s，最大 10 万条）
- **TLS 加密**：独立端口 9001 + SslHandler + 自签名证书兜底
- **可观测性**：Micrometer + Prometheus，9 项核心指标
- **消息路由**：`MessageDispatcher` + `MessageHandler` 接口
- **断线重连**：指数退避 1s→30s

### Changed
- 协议帧头部 11→15 字节，新增 `sequenceId`(4B)
- Version 字段默认值 1→2
- 新增消息类型：AUTH_REQ(4)/AUTH_RESP(5)/AUTH_FAIL(6)/ACK(7)

### Added Dependencies
- `spring-boot-starter-data-redis`
- `caffeine`
- `spring-kafka`
- `spring-boot-starter-actuator`
- `micrometer-registry-prometheus`

---

## [1.0.0] - 2026-06-13

### Added
- **自定义协议（DVSR）**：15 字节帧头（Magic/Ver/Serial/Type/SeqId/Length），V1/V2 自动兼容
- **多协议探测器**：`MultiProtocolDetector`（DVSR/HTTP/MQTT 嗅探 + 动态路由）
- **心跳检测**：5s 读空闲，3 次连续空闲断开
- **Spring Boot 3.2.5 集成**：配置外化、生命周期管理
- **交互式测试客户端**：`TestClient`（鉴权 + 断线重连）
- **压力测试客户端**：`StressTestClient`（万级连接）
- Netty 4.1.109、Java 17、Jackson JSON 序列化

---

## Technology Stack

| Component | Version | Since |
|-----------|---------|-------|
| Java | 17 | v1.0.0 |
| Spring Boot | 3.2.5 | v1.0.0 |
| Netty | 4.1.109.Final | v1.0.0 |
| Redis (Lettuce) | Spring Boot managed | v1.0.0 |
| Caffeine | Spring Boot managed | v2.0.0 |
| Kafka (Spring Kafka) | Spring Boot managed | v3.0.0 |
| Guava | 32.1.3-jre | v3.0.0 |
| Protobuf | 3.25.3 | v3.0.0 |
| Eclipse Milo (OPC-UA) | 0.6.13 | v4.1.0 |
| Jasypt | 3.0.5 | v5.0.0 |
| JaCoCo | 0.8.11 | v5.0.0 |

## Test Coverage Evolution

| Version | Test Cases | Line Coverage |
|---------|-----------|---------------|
| v1.0.0 | 6 | ~5% |
| v5.0.0 | 75 | 31.7% |
| v5.1.0 | 130 | 54.3% |
