# Device Server - 工业级 Netty 多协议设备接入服务器

基于 Spring Boot 3 + Netty 构建的工业级多协议设备接入服务器，支持 DVSR 自定义协议、Modbus-TCP、OPC-UA、HTTP、WebSocket、MQTT 六种协议接入，内置设备鉴权、HasdedWheelTimer ACK、TCP 背压流控、IP 动态黑名单、优雅停机、分布式集群路由等生产级特性。

---

## 1. 技术栈

| 组件 | 版本 | 说明 |
|------|------|------|
| Java | 17 | LTS 版本 |
| Spring Boot | 3.2.5 | 依赖注入、配置管理、生命周期 |
| Netty | 4.1.109.Final | 网络通信框架 |
| Jackson | Spring Boot 内置 | JSON 序列化 |
| Spring Data Redis | Spring Boot 内置 | Lettuce 异步客户端，设备鉴权密钥存储 |
| Caffeine | Spring Boot 内置 | 本地缓存，待确认消息 TTL 管理 |
| Spring Kafka | Spring Boot 内置 | 消息持久化，统一 Topic + deviceId 分区 |
| Guava | 32.1.3-jre | 令牌桶限流（RateLimiter） |
| Protobuf | 3.25.3 | 二进制序列化（高性能，与 JSON 并存） |
| Eclipse Milo | 0.6.13 | OPC-UA SDK Server |
| Micrometer + Prometheus | Spring Boot 内置 | 可观测性指标暴露 |
| Lombok | Spring Boot 内置 | 减少样板代码 |

---

## 2. 架构设计

### 2.1 整体架构

```
[设备/客户端] ──TCP/TLS──> [Netty Server]
                               │
                         [SslHandler]          ← TLS 加密（独立端口 9001）
                               │
                         [IpFilterHandler]     ← V4 IP 动态黑名单
                               │
                         [ReadTimeoutHandler]   ← 鉴权超时检测
                               │
                         [BackpressureHandler]  ← V4 TCP 背压流控
                               │
                         [IdleStateHandler]     ← 空闲检测
                               │
                         [MultiProtocolDetector] ← 6协议嗅探，动态路由
                               │
          ┌────────┬──────────┼──────────┬──────────┬──────────┐
          ▼        ▼          ▼          ▼          ▼          ▼
     [DVSR]   [Modbus]     [OPC-UA]   [HTTP/WS]  [MQTT]    (未知拉黑)
          │        │          │          │          │
     [AuthHandler]  │          │          │          │
          │        │          │          │          │
   [RateLimitHandler] │        │          │          │
          │        │          │          │          │
   [CustomHandler][ModbusHdr][OpcUaHdr][HttpHdr] [MqttHdr]
          │        │          │          │          │
          └────────┴──────────┴──────────┴──────────┘
                              │
                       [MessageDispatcher]
                              │
          [ThingModelMessageHandler] (V4 物模型前置转换)
                              │
                 [BusinessMessageHandler] → [Kafka]
                              │
                       [businessGroup]     ← 独立线程池
```

### 2.2 线程模型

```
bossGroup (1线程)        → 接收新连接（Acceptor）
workerGroup (CPU×2线程)   → I/O 读写（EventLoop）
businessGroup (64线程)    → 业务处理（独立线程池，防阻塞 I/O）
```

### 2.3 自定义协议帧结构（V2）

```
+------------+--------+-----------+---------+-------------+-----------+---------+
| Magic(4B)  | Ver(1B)| Serial(1B)| Type(1B)| SeqId(4B)   | Length(4B)| Body(N) |
+------------+--------+-----------+---------+-------------+-----------+---------+
| 0x44565352 |   2    |  1=JSON   | 1-7     | 消息序列号    | Body长度   | 变长数据 |
|   "DVSR"   |        | 2=Protobuf|         | 0=不需确认   |           |         |
+------------+--------+-----------+---------+-------------+-----------+---------+
```

| 字段 | 长度 | 说明 |
|------|------|------|
| Magic Number | 4 字节 | 协议标识 `0x44565352` ("DVSR") |
| Version | 1 字节 | 协议版本号（1=V1无SeqId, 2=V2含SeqId） |
| Serialization Type | 1 字节 | 序列化方式：1=JSON，2=Protobuf(预留) |
| Msg Type | 1 字节 | 消息类型（见下表） |
| Sequence ID | 4 字节 | 消息序列号：0=不需要确认，>0=需要ACK确认 |
| Length | 4 字节 | Body 的字节长度 |
| Body | 变长 | 实际数据（心跳为字符串，鉴权为JSON，业务走序列化） |

V2 固定头部共 **15 字节**，V1 固定头部共 **11 字节**（无 Sequence ID），服务端自动兼容。

### 2.4 消息类型定义

| msgType | 名称 | 说明 |
|---------|------|------|
| 1 | HEARTBEAT_REQ | 心跳请求（客户端发送 "PING"） |
| 2 | HEARTBEAT_RESP | 心跳响应（服务端回复 "PONG"） |
| 3 | BUSINESS | 业务数据 |
| 4 | AUTH_REQ | 鉴权请求（设备上线首包） |
| 5 | AUTH_RESP | 鉴权成功响应 |
| 6 | AUTH_FAIL | 鉴权失败响应 |
| 7 | ACK | 消息确认 |
| 8 | VERSION_NEGOTIATE | 协议版本协商 |

### 2.5 设备鉴权机制

```
设备出厂烧录 ProductSecret
        │
        ▼
设备上电: token = HMAC-MD5(deviceId + timestamp, productSecret)
        │
        ▼
首包 AUTH_REQ: Body = { deviceId, timestamp, token }
        │
        ▼
服务端 Redis 异步读取 productSecret (Lettuce, <2ms)
        │
        ▼
计算 HMAC 比对 token → 通过/拒绝
        │
        ├── 通过: deviceId 绑定 Channel 属性, AuthHandler 从 Pipeline 移除
        │         后续报文不再携带 deviceId, 从 Channel 属性直接获取
        │
        └── 拒绝: 回复 AUTH_FAIL, 关闭连接
```

**鉴权超时**：连接建立后 10 秒内未发送 AUTH_REQ 则断开（`ReadTimeoutHandler`）。

**防重放攻击**：timestamp 与服务端时间差超过 5 分钟视为无效。

**Redis Key 设计**：`device:auth:{deviceId}` → `productSecret`

### 2.6 连接管理

```
AuthHandler 鉴权成功
        │
        ▼
DeviceChannelManager.register(deviceId, channel)
        │
        ├── 新设备 → 注册映射
        └── 重复登录 → 踢掉旧连接（oldChannel.close()），注册新连接

CustomProtocolHandler.channelInactive
        │
        ▼
DeviceChannelManager.unregister(deviceId, channel)
        │
        └── 仅当 Channel 引用一致时移除（避免新连接被旧断开事件误删）
```

### 2.7 消息确认机制

```
发送方: sequenceId > 0 的消息 → PendingAckManager(Caffeine TTL 30s)
        │
        ▼
接收方收到 → 回复 ACK(msgType=7, body=sequenceId)
        │
        ▼
发送方收到 ACK → 从 PendingAckManager 移除

超时未确认 → Caffeine TTL 自动淘汰 → 日志告警（不自动重传，避免雪崩）
```

- `sequenceId = 0`：不需要确认（心跳、鉴权等）
- `sequenceId > 0`：需要 ACK 确认（关键业务数据）
- Caffeine 最大容量 10 万条，TTL 30 秒，内存占用可控

### 2.8 多协议探测机制

`MultiProtocolDetector` 位于 Pipeline 前端，通过嗅探数据包前几个字节判断协议类型：

| 协议 | 嗅探规则 | 动态插入的 Handler |
|------|----------|-------------------|
| 自定义协议 | 魔数 `0x44565352` ("DVSR") | CustomProtocolDecoder + CustomProtocolEncoder |
| HTTP | 首字母匹配 GET/POST/PUT/DELETE/HEAD/PATCH/OPTIONS | HttpServerCodec + HttpObjectAggregator |
| MQTT | 首字节高4位=1 (CONNECT 报文) | MqttDecoder + MqttEncoder |
| 未知 | 不匹配以上规则 | 直接关闭连接 |

### 2.9 心跳检测机制

- **5秒** 无读数据触发空闲事件
- 连续 **3次** 空闲才断开，避免网络瞬时抖动造成误杀
- 收到任何数据（含心跳响应）立即重置计数器
- 客户端 **5秒** 写空闲自动发送 PING 保活

### 2.10 TLS 加密

- 独立端口（默认 9001），与明文端口（9000）分离
- 未配置证书时自动使用自签名证书（仅限开发环境）
- `SslHandler` 在 Pipeline 最前端，解密后不影响协议探测

### 2.11 消息路由分发 + Kafka 持久化

```
CustomProtocolHandler.channelRead0()
        │
        ▼
MessageDispatcher.dispatch(ctx, deviceId, packet)
        │
        ├── msgType=3 → BusinessMessageHandler (Spring Bean)
        │   └── 构造 KafkaMessageEnvelope → KafkaProducerService.sendAsync()
        ├── msgType=X → 自定义 MessageHandler 实现
        └── 未注册 → 默认日志处理
```

新增业务类型只需实现 `MessageHandler` 接口并注册为 Spring Bean，无需修改核心 Handler。
Kafka 启用时自动异步写入统一 Topic `device-messages`，key=deviceId 保证同一设备消息有序。

### 2.12 流量控制（令牌桶）

```
CustomProtocolHandler
        │
        ▼
RateLimitHandler（鉴权后插入 Pipeline）
        │
        ├── 心跳消息 → 无限流，直接放行
        ├── 未认证 → 无限流（由 AuthHandler 管理）
        └── 业务消息 → RateLimiterService.tryAcquire(deviceId)
                ├── 全局限流：单一 RateLimiter（默认 10000/s）
                ├── 单设备限流：Caffeine 缓存 deviceId→RateLimiter（默认 100/s）
                ├── 通过 → ctx.fireChannelRead(packet)
                └── 拒绝 → 丢弃消息/关闭连接（可配置）
```

### 2.13 WebSocket 支持

```
MultiProtocolDetector（HTTP 分支）
        │
        ├── WebSocket 启用 → 动态插入：
        │   HttpServerCodec → HttpObjectAggregator → WebSocketServerProtocolHandler
        │   → WebSocketBusinessHandler
        │
        └── WebSocket 禁用 → 走原有 HTTP 路径（HttpBusinessHandler）
```

WebSocket 升级路径 `/ws`，二进制帧体格式：serializationType(1B) + msgType(1B) + sequenceId(4B) + payload(NB)。
消息路由复用 MessageDispatcher，与自定义协议共享处理逻辑。

### 2.14 Protobuf 序列化

通过协议帧头部的 `serializationType=2` 标识 Protobuf 编码，`ProtobufSerializer` 实现 `Serializer` 接口。
`.proto` 文件通过 `protobuf-maven-plugin` 编译生成 Java 类，运行时 Class→Parser 动态映射。

### 2.15 协议版本协商

```
AuthHandler 允许 VERSION_NEGOTIATE(msgType=8) 提前通过
        │
        ▼
VersionNegotiator.negotiate(clientVersion)
        │
        ├── clientVersion < 1 → 返回 -1（不兼容），关闭连接
        └── clientVersion ≥ 1 → 返回 min(clientVersion, 2)，绑定到 Channel 属性
```

协商后版本绑定到 `ChannelAttributes.NEGOTIATED_VERSION`，编解码器按此版本执行。
不支持 VERSION_NEGOTIATE 的客户端默认使用 V2。

### 2.16 断线重连（客户端）

```
连接断开 → 触发 channelInactive
        │
        ▼
指数退避重连: 1s → 2s → 4s → 8s → 16s → 30s（最大）
        │
        ▼
重连成功 → 重置退避 → 重新发送 AUTH_REQ
```

### 2.17 HashedWheelTimer ACK 优化（V4）

```
V3.x: Caffeine TTL 缓存，后台线程批量清理 → 高并发 GC 压力
V4.x: Netty HashedWheelTimer，单线程轮询 O(1) 超时检测
      双 ConcurrentHashMap + LongAdder 精确计数
      轮盘 2048 槽位，100ms tick
```

ACK 写入 → packetMap + timeoutMap。ACK 到达 → 双表移除 + timeout.cancel()。超时 → 日志告警（不自动重传避免雪崩）。

### 2.18 TCP 背压流控（V4）

```
BackpressureHandler
        │
        ▼
channelWritabilityChanged 事件双向驱动
        │
        ├── 高水位(64KB) → setAutoRead(false)，停止读 TCP 缓冲区
        │                    TCP 滑动窗口收缩 → 设备降频
        │
        └── 低水位(32KB) → setAutoRead(true)，恢复通信
```

基于 Netty Channel 的 `WRITE_BUFFER_HIGH_WATER_MARK` 和 `LOW_WATER_MARK` 配置。

### 2.19 IP 动态黑名单（V4）

```
IpFilterHandler（Pipeline 最前端）
        │
        ▼
IpfirewallService.isBanned(ip)
        │
        ├── 本地 Caffeine 缓存命中 → ctx.close()
        ├── Redis 黑名单命中 → ctx.close()
        └── 放行 → 后续 Handler 处理
                │
                ▼
        MultiProtocolDetector 未知协议 → recordFailure(ip)
        ip 计数超过阈值(60s/5次) → Redis 黑名单(TTL 30min)
```

### 2.20 优雅停机与离线遗言（V4）

```
Spring @PreDestroy
        │
        ▼
  0. 关闭监听端口（拒绝新连接）
  1. channelManager.broadcastMaintenanceNotice()  ← 广播维护通知
  2. kafkaProducerService.flushBuffer(5s)          ← 冲刷 Kafka 缓冲器
  3. channelManager.closeAll()                     ← 关闭所有连接
  4. boss/worker/business group.shutdownGracefully  ← 释放线程组
```

### 2.21 分布式集群路由（V4）

```
Node-1               Node-2               Node-3
  │                    │                    │
  ├─ DeviceChannelManager (本地)
  │   └─ ClusterSessionManager (Redis)
  └─ ClusterRouterService (Pub/Sub)
            │                    │
      ┌─────▼────────────────────▼─────┐
      │            Redis               │
      │  Hash: device:session:{id}     │  ← nodeId 路由表
      │  PubSub: cluster:command:{node}│  ← 跨节点指令
      └────────────────────────────────┘
```

Lua 原子令牌校验注销，防止节点闪断竞态误删。

### 2.22 Modbus-TCP 协议支持（V4）

```
MultiProtocolDetector
        │
        ▼
isModbusTcp(b2/b3=0x0000 + b4=0x00 + b5∈(0,255])
        │
        ▼
ModbusEncoder → ModbusDecoder → ModbusBusinessHandler
                                    │
                    支持功能码: 01(读线圈) 02(读离散输入)
                              03(读保持寄存器) 04(读输入寄存器)
                              06(写单寄存器) 10(写多寄存器)
```

### 2.23 OPC-UA 协议支持（V4）

```
MultiProtocolDetector
        │
        ▼
isOpcUa(HEL/ACK/OPN/MSG/CLO/ERR 消息类型识别)
        │
        ▼
OpcUaBusinessHandler
        │
  状态机: HEL → ACK | OPN → 记录通道 | MSG → 透传 | CLO → 关闭
```

集成 Eclipse Milo SDK 0.6.13。

### 2.24 物模型插件接口（V4）

```
设备原始二进制数据 (Modbus Hex / OPC-UA Variant / DVSR byte[])
        │
        ▼
ThingModelMessageHandler (业务方实现)
        │
        ▼
convertToThingModel(ThingModelContext) → 标准物模型 JSON
        │
        ▼
回填 packet.body → BusinessMessageHandler → Kafka
```

零侵入设计：实现接口 + `@Component` → DispatcherConfig 自动装配。
## 3. 项目结构

```
src/main/java/com/xsh/netty/
├── DeviceServerApplication.java          # Spring Boot 主启动类
├── protocol/
│   ├── MessageHeader.java                # 协议头定义（V1/V2 兼容）
│   ├── MessagePacket.java                # 消息包（Header + Body + rawBody）
│   ├── MsgType.java                      # 消息类型常量（8种）
│   ├── AuthRequest.java                  # 鉴权请求体
│   ├── ChannelAttributes.java            # Channel 属性键
│   ├── VersionInfo.java                  # 版本常量
│   └── VersionNegotiator.java            # 版本协商器
├── serialize/
│   ├── Serializer.java                   # 序列化接口（JSON=1, Protobuf=2）
│   ├── JsonSerializer.java               # JSON 序列化
│   └── ProtobufSerializer.java           # Protobuf 序列化
├── codec/
│   ├── CustomProtocolEncoder.java        # DVSR 协议编码器
│   ├── CustomProtocolDecoder.java        # DVSR V1/V2 自适应解码器
│   ├── MultiProtocolDetector.java        # 6协议探测器（DVSR/Modbus/OPC-UA/HTTP/MQTT）
│   ├── WebSocketFrameCodec.java          # WebSocket 帧转换器
│   ├── ModbusFrame.java                  # Modbus 数据帧 POJO                        [V4]
│   ├── ModbusDecoder.java                # Modbus MBAP 解码器                        [V4]
│   └── ModbusEncoder.java                # Modbus MBAP 编码器                        [V4]
├── auth/
│   ├── AuthService.java                  # 鉴权服务接口
│   ├── HmacUtils.java                    # HMAC-MD5 工具类
│   └── RedisAuthService.java             # Redis 异步鉴权 + 启动连通性校验
├── handler/
│   ├── AuthHandler.java                  # 鉴权处理器 + 版本协商
│   ├── CustomProtocolHandler.java        # DVSR 业务处理器
│   ├── BackpressureHandler.java          # TCP 背压流控                             [V4]
│   ├── IpFilterHandler.java              # IP 动态黑名单                             [V4]
│   ├── BusinessMessageHandler.java       # Kafka 持久化 + Protobuf 区分
│   ├── WebSocketBusinessHandler.java     # WebSocket 鉴权 + 路由
│   ├── ModbusBusinessHandler.java        # Modbus 6种功能码                          [V4]
│   ├── OpcUaBusinessHandler.java         # OPC-UA HEL/OPN/MSG 状态机                 [V4]
│   ├── HttpBusinessHandler.java          # HTTP 业务（预留）
│   └── MqttBusinessHandler.java          # MQTT 业务（预留）
├── server/
│   ├── NettyServerProperties.java        # 配置属性（45+ 配置项）
│   ├── DeviceChannelManager.java         # 连接管理（优雅停机广播 + 全关闭）
│   ├── DeviceSession.java                # 设备会话
│   ├── PendingAckManager.java            # HasdedWheelTimer ACK 管理                 [V4]
│   ├── IpFirewallService.java            # IP 防火墙（Redis + Caffeine）              [V4]
│   ├── ClusterSessionManager.java        # 集群会话（Lua 原子注销）                    [V4]
│   └── ClusterRouterService.java         # 集群路由（Redis Pub/Sub）                  [V4]
├── dispatcher/
│   ├── MessageHandler.java               # 业务消息处理器接口
│   ├── MessageDispatcher.java            # 消息分发器
│   ├── ThingModelContext.java            # 物模型上下文                              [V4]
│   └── ThingModelMessageHandler.java     # 物模型插件接口                             [V4]
├── ratelimit/
│   └── RateLimiterService.java           # 双维度令牌桶
├── kafka/
│   ├── KafkaProducerService.java         # Kafka 异步发送
│   ├── KafkaProducerConfig.java          # Kafka 条件装配
│   └── KafkaMessageEnvelope.java         # 消息信封
├── config/
│   ├── NettyServerBootstrap.java         # 服务启动 + 优雅停机 + 集群强防御校验
│   ├── NettyMetricsBinder.java           # 12项 Micrometer 指标
│   ├── DispatcherConfig.java             # 分发器 + 物模型自动装配
│   └── HandlerBeanContainer.java         # Handler Bean 依赖容器
└── client/
    ├── TestClient.java                   # 交互式测试客户端
    └── StressTestClient.java             # 压力测试客户端
```

---

## 4. 配置说明

`application.yml` 完整配置：

```yaml
server:
  port: 8080                          # Spring Boot HTTP 端口（管理用）

netty:
  server:
    port: 9000                        # Netty TCP 明文监听端口
    boss-threads: 1                   # Acceptor 线程数（1个足够）
    worker-threads: 0                 # I/O 线程数（0=默认 CPU核心数×2）
    business-threads: 64              # 业务处理线程池大小
    idle-timeout-seconds: 5           # 读空闲超时（秒）
    max-idle-count: 3                 # 最大连续空闲次数
    max-frame-length: 10485760        # 单帧最大长度（10MB），防 OOM
    so-backlog: 1024                  # TCP 连接排队数
    tls-enabled: false                # TLS 开关
    tls-port: 9001                    # TLS 独立端口
    tls-cert-path: ""                 # TLS 证书路径（PKCS12/JKS）
    tls-cert-password: ""             # TLS 证书密码
    auth-timeout-seconds: 10          # 鉴权超时（秒），超时未鉴权断开

spring:
  data:
    redis:
      host: 127.0.0.1                # Redis 地址
      port: 6379                      # Redis 端口
      password: ""                    # Redis 密码
      lettuce:
        pool:
          max-active: 16             # 连接池最大连接数
          max-idle: 8                 # 连接池最大空闲连接数
          min-idle: 2                 # 连接池最小空闲连接数

management:
  endpoints:
    web:
      exposure:
        include: health, prometheus   # 暴露健康检查和 Prometheus 指标
  endpoint:
    health:
      show-details: always
```

---

## 5. 安全防护

| 防护点 | 实现方式 |
|--------|----------|
| 设备鉴权 | HMAC-MD5 + Redis 异步校验，首包必须为 AUTH_REQ |
| 鉴权超时 | `ReadTimeoutHandler` 10 秒未鉴权断开 |
| 防重放攻击 | timestamp 偏差超过 5 分钟拒绝 |
| 非法协议 | 魔数校验失败直接关闭连接 |
| 超大帧攻击 | `maxFrameLength` 上限校验（10MB），超过关闭连接 |
| 负数长度 | length < 0 直接关闭连接 |
| 恶意扫描 | 未知协议直接关闭连接 |
| 未认证访问 | AuthHandler 拦截所有非 AUTH_REQ 消息 |
| TLS 加密 | 独立端口，SslHandler 加密传输 |
| I/O 阻塞 | 业务 Handler 在独立 `businessGroup` 线程池执行 |
| 内存泄漏 | Netty 内置 ByteBuf 自动释放；异常路径统一 `ctx.close()` |
| 消息确认超时 | Caffeine TTL 自动淘汰，最大 10 万条，不无限增长 |

---

## 6. 可观测性

### 暴露的 Prometheus 指标

访问 `http://localhost:8080/actuator/prometheus` 获取：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `netty_connections_active` | Gauge | 当前活跃连接数 |
| `netty_ack_pending` | Gauge | 待确认消息数 |
| `netty_heartbeat_request_total` | Counter | 心跳请求计数 |
| `netty_heartbeat_response_total` | Counter | 心跳响应计数 |
| `netty_business_message_total` | Counter | 业务消息计数 |
| `netty_auth_success_total` | Counter | 鉴权成功计数 |
| `netty_auth_fail_total` | Counter | 鉴权失败计数 |
| `netty_business_message_latency_seconds` | Timer | 消息处理延迟分布 |
| `netty_kafka_send_success_total` | Counter | Kafka 发送成功计数 |
| `netty_kafka_send_fail_total` | Counter | Kafka 发送失败计数 |
| `netty_rate_limited_total` | Counter | 限流拒绝计数 |
| `netty_websocket_connection_total` | Counter | WebSocket 连接计数 |

---

## 7. 测试方案

### 7.1 单元测试

已实现的单元测试：

| 测试类 | 测试项 | 数量 |
|--------|--------|------|
| `CustomProtocolCodecTest` | 心跳编解码、业务编解码、非法魔数、超大帧、半包处理 | 5 |
| `NettyServerPropertiesTest` | 默认配置值校验 | 1 |

运行方式：

```bash
mvn test
```

### 7.2 交互式测试

使用 `TestClient` 进行手动验证（含鉴权 + 断线重连）：

```bash
# 默认连接 127.0.0.1:9000，使用测试设备ID和密钥
java com.xsh.netty.client.TestClient

# 指定地址、端口、设备ID和密钥
java com.xsh.netty.client.TestClient 192.168.1.100 9000 test-device-001 my-secret-key
```

操作方式：
- 连接建立后自动发送鉴权请求
- 鉴权通过后自动发送心跳保活
- 控制台输入文本回车发送业务数据
- 断线后自动重连（指数退避）
- 输入 `quit` 退出

### 7.3 十六进制报文测试

使用网络调试工具（NetAssist、PacketSender 等）发送十六进制报文：

**V2 鉴权请求（Body 为 AuthRequest JSON）：**
```
44 56 53 52   # Magic: "DVSR"
02            # Version: 2
01            # Serialization: JSON
04            # MsgType: AUTH_REQ
00 00 00 00   # SequenceId: 0 (不需要确认)
00 00 00 XX   # Length: Body长度
7B ... 7D     # Body: {"deviceId":"test-001","timestamp":1718500000000,"token":"abcdef..."}
```

**V2 心跳请求 "PING"：**
```
44 56 53 52   # Magic: "DVSR"
02            # Version: 2
01            # Serialization: JSON
01            # MsgType: 心跳请求
00 00 00 00   # SequenceId: 0
00 00 00 04   # Length: 4
50 49 4E 47   # Body: "PING"
```

**V1 心跳请求（兼容旧客户端）：**
```
44 56 53 52   # Magic: "DVSR"
01            # Version: 1 (V1无SequenceId)
01            # Serialization: JSON
01            # MsgType: 心跳请求
00 00 00 04   # Length: 4
50 49 4E 47   # Body: "PING"
```

### 7.4 多协议测试

| 协议 | 推荐工具 | 说明 |
|------|----------|------|
| 自定义协议 | TestClient / 网络调试助手 | 发送十六进制报文，需先鉴权 |
| HTTP | Postman / curl | 发送 GET/POST 请求到 9000 端口 |
| WebSocket | WebSocket 客户端 / 浏览器控制台 | `ws://localhost:9000/ws`，首条消息 AUTH_REQ |
| MQTT | MQTTX | 连接 9000 端口，发送 CONNECT 报文 |

### 7.5 TLS 测试

```bash
# 启用 TLS（application.yml 中设置 netty.server.tls-enabled=true）
# 使用 openssl 连接 TLS 端口
openssl s_client -connect 127.0.0.1:9001
```

### 7.6 压力测试

使用 `StressTestClient` 模拟大量并发连接：

```bash
# 默认：10000连接，每批100个，间隔50ms，不发送业务数据
java com.xsh.netty.client.StressTestClient

# 自定义参数：主机 端口 连接数 每批大小 批次间隔(ms) 业务发送间隔(秒,0=不发送)
java com.xsh.netty.client.StressTestClient 192.168.1.100 9000 10000 100 50 30
```

压测客户端特性：
- 所有连接共享 EventLoopGroup，万级连接仅需 CPU×2 个线程
- 逐步建连（ramp-up），避免瞬间冲击
- 自动心跳保活（5秒写空闲发送 PING）
- 可配置周期向所有连接群发业务数据
- 实时统计：活跃数、成功/失败/断开、心跳收发、业务发送

#### 万级连接前的系统调优

**Linux：**
```bash
ulimit -n 65535
```

**Windows：**
```powershell
# 管理员权限运行 PowerShell

# 扩大临时端口范围（默认 5000 → 65534）
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" `
    -Name "MaxUserPort" -Value 65534 -PropertyType DWord -Force

# 缩短 TIME_WAIT 时间（默认 120s → 30s，加快端口回收）
New-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip\Parameters" `
    -Name "TcpTimedWaitDelay" -Value 30 -PropertyType DWord -Force

# 重启生效
Restart-Computer
```

---

## 8. 快速启动

### 8.1 前置依赖

- Java 17+
- Redis（设备鉴权密钥存储）
- Maven 3.6+

### 8.2 Redis 准备

设备激活时，将 deviceId 和 productSecret 同步到 Redis：

```bash
# 示例：注册设备 test-device-001，密钥为 my-secret-key
redis-cli SET "device:auth:test-device-001" "my-secret-key"
```

### 8.3 编译

```bash
mvn clean package -DskipTests
```

### 8.4 启动服务端

```bash
java -jar target/device-server-1.0.0-SNAPSHOT.jar
```

启动成功后日志输出：
```
Netty 明文服务启动，端口: 9000 (Epoll: false)
Netty TLS 服务启动，端口: 9001    # 如果启用了 TLS
```

### 8.5 启动测试客户端

```bash
# 交互式测试（含鉴权 + 断线重连）
java -cp target/device-server-1.0.0-SNAPSHOT.jar com.xsh.netty.client.TestClient 127.0.0.1 9000 test-device-001 my-secret-key

# 压力测试
java -cp target/device-server-1.0.0-SNAPSHOT.jar com.xsh.netty.client.StressTestClient
```

---

## 9. 生产环境注意事项

1. **Linux 部署启用 Epoll**：自动检测，需确保系统安装 `libnetty-transport-native-epoll`
2. **内存泄漏防护**：业务代码中手工创建的 `ByteBuf` 必须遵循 `ReferenceCountUtil.release(msg)`
3. **业务隔离**：禁止在 I/O 线程中做数据库查询、RPC 调用、复杂计算，必须提交到 `businessGroup`
4. **帧长度限制**：`maxFrameLength` 根据实际业务调整，防止恶意客户端导致 OOM
5. **日志级别**：生产环境建议将 Netty 相关日志设为 `WARN`，避免大量心跳日志
6. **TLS 证书**：生产环境必须配置正式证书，不要使用自签名证书
7. **Redis 高可用**：鉴权依赖 Redis，建议使用 Redis Sentinel 或 Cluster
8. **鉴权密钥管理**：设备激活时通过业务系统将 productSecret 同步到 Redis，Netty 网关不直接管理密钥
9. **Kafka 条件装配**：`kafka-enabled=false` 时 Kafka Bean 不创建，避免未配置 Kafka 导致启动失败
10. **启动健康检查**：启动时校验 Redis PING，不可用时告警不阻塞
11. **Grafana JVM 监控**：Dashboard 含堆内存/GC/线程/CPU 面板
12. **IP 动态黑名单**：60s/5次恶意行为自动拉黑，Redis + Caffeine 双层缓存
13. **TCP 背压流控**：下游积压时通过 TCP 滑动窗口反压设备降频，防 OOM
14. **Lua 原子集群注销**：分布式集群下防止网络闪断竞态误删合法路由

---

## 10. 演进路线

| 阶段 | 状态 | 内容 |
|------|------|------|
| V1.0 基础框架 | ✅ 已完成 | 编解码、多协议接入、心跳检测、Spring Boot 集成 |
| P0 生产必须 | ✅ 已完成 | 设备鉴权(Redis+HMAC)、连接管理(踢旧保新)、消息确认(Caffeine TTL)、TLS 加密 |
| P1 运维必备 | ✅ 已完成 | 可观测性(Micrometer+Prometheus)、消息路由分发、断线重连(指数退避) |
| P2 规模化 | ✅ 已完成 | 流量控制(令牌桶)、消息持久化(Kafka)、协议版本协商 |
| P3 扩展功能 | ✅ 已完成 | WebSocket、Protobuf、Grafana 看板 |
| V4.0 生产级 | ✅ 已完成 | HashedWheelTimer ACK、TCP背压流控、IP动态黑名单、优雅停机、分布式集群路由 |
| V4.1 工业协议 | ✅ 已完成 | Modbus-TCP(MBAP+6功能码)、OPC-UA(HEL/OPN/MSG)、物模型插件接口 |

---

## 11. 版本变更记录

### V4.1 (当前) — 工业协议全覆盖

**阶段 6-8 新增：**
- Modbus-TCP：MBAP 增强嗅探（b2/b3=0x0000 + b4=0x00）+ 6 种功能码（01/02/03/04/06/10）
- OPC-UA：HEL/ACK/OPN/MSG/CLO/ERR 消息类型识别 + Eclipse Milo SDK 0.6.13
- 物模型插件：ThingModelMessageHandler 接口 + ThingModelContext 上下文，零侵入自动装配
- MultiProtocolDetector：6 协议全覆盖（DVSR/Modbus/OPC-UA/HTTP/WS/MQTT）

### V4.0 — 生产级高并发基础

**阶段 1-5 新增：**
- HashedWheelTimer ACK：双 Map + LongAdder 替代 Caffeine，O(1) 超时检测，2048 槽位
- TCP 背压流控：channelWritabilityChanged 双向驱动，高水位 64KB/低水位 32KB
- IP 动态黑名单：Redis+Caffeine，60s/5次自动拉黑，fail-open 安全策略
- 优雅停机：广播维护通知 + Kafka flush 冲刷 + shutdownGracefully 超时隔离
- 分布式集群：ClusterSessionManager Lua 原子注销 + ClusterRouterService Pub/Sub 路由 + node-id 强防御
- KafkaProducerService.flushBuffer()：替代 CompletableFuture.join() 避免死锁

**新增依赖：** guava 32.1.3-jre（版本兼容调整）、Eclipse Milo 0.6.13

### V3.1 — 生产级安全与健壮性修复

**P0 安全漏洞修复：**
- **token 明文泄露**：`RedisAuthService` 删除 token 明文日志（log.info → log.debug 脱敏）
- **WebSocket 鉴权补全**：从 TODO 存根实现完整鉴权流程（AuthRequest 反序列化 → AuthService 异步校验 → deviceId 绑定 → channelManager 注册）
- **Kafka 条件装配**：`KafkaProducerConfig`/`KafkaProducerService` 增加 `@ConditionalOnProperty`，禁用时 Bean 不创建、应用不崩溃
- **HandlerBeanContainer 可选依赖**：`KafkaProducerService` 改为 `@Autowired(required=false)` 注入
- **安全默认值**：`kafka-enabled` 默认 `false`

**P1 健壮性增强：**
- **dispatch 异常保护**：`CustomProtocolHandler` BUSINESS 分支 try-catch，单消息异常不导致连接断开
- **stop() 资源隔离**：`NettyServerBootstrap.stop()` 每个资源关闭独立 try-catch，确保全部释放
- **日志级别优化**：`MultiProtocolDetector` 3处协议检测 info→debug，`AuthHandler` 版本协商 info→debug

**P2 质量修复：**
- **Protobuf 序列化区分**：`BusinessMessageHandler` 按 `serializationType` 区分处理（Protobuf→Base64 编码，JSON→UTF-8），body null 防御
- **版本协商默认值**：`AuthHandler` NumberFormatException 不再默认 V2，改为关闭连接
- **Guava 版本兼容**：降级到 32.1.3-jre（与 Spring Boot 3.2.5 兼容）

**P3 运维增强：**
- **JVM 指标面板**：Grafana Dashboard 新增 4 个面板（堆内存、GC 频率/耗时、线程数、CPU 使用率）
- **Redis 启动校验**：`RedisAuthService` @PostConstruct 执行 PING 连通性检查
- **依赖管理**：`maven-enforcer-plugin` banDuplicatePomDependencyVersions 规则

### V3.0

**新增功能：**
- Kafka 消息持久化：BusinessMessageHandler → 统一Topic `device-messages`，deviceId 分区保序
- 流量控制：全局+单设备双维度令牌桶（Guava RateLimiter），心跳不限流
- WebSocket 支持：复用 HTTP 端口升级，二进制帧体与自定义协议共享 MessagePacket 格式
- Grafana 看板：Dashboard JSON 模板，12 块面板覆盖连接/心跳/业务/鉴权/JVM 等核心指标
- Protobuf 序列化：`serializationType=2` 自动路由，`.proto` 编译生成 Java 类
- 协议版本协商：VERSION_NEGOTIATE(msgType=8) 动态协商，向后兼容
- HandlerBeanContainer：解决 Netty Handler 无法注入 Spring Bean 的架构问题
- BusinessMessageHandler：Spring Bean 实现 MessageHandler 接口，Dispatcher 自动注册
- RateLimitHandler：Pipeline 插入式限流，心跳不限流

**遗留修复：**
- CustomProtocolHandler BUSINESS 消息接入 MessageDispatcher
- ACK 处理接入 PendingAckManager.ack()
- NettyMetricsBinder 指标接入各 Handler（AuthHandler、CustomProtocolHandler）
- 业务消息反序列化改为 byte[] 透传（延迟到 MessageHandler 按需执行）

**新增依赖：**
- guava（令牌桶限流 RateLimiter）
- protobuf-java + protobuf-maven-plugin（Protobuf 序列化）

**新增配置项：**
- `netty.server.kafka-enabled` / `kafka-topic`
- `netty.server.rate-limit-enabled` / `rate-limit-global-permits` / `rate-limit-device-permits` / `rate-limit-close-on-limit`
- `netty.server.websocket-enabled` / `websocket-path` / `websocket-max-frame-size`
- `spring.kafka.bootstrap-servers` / `producer.*`

**新增 Grafana Dashboard：**
- `src/main/resources/grafana/device-server-dashboard.json`

### V2.0

**协议变更：**
- 协议帧头部从 11 字节扩展到 15 字节，新增 `sequenceId`(4字节) 字段
- Version 字段默认值从 1 改为 2
- 新增消息类型：AUTH_REQ(4)、AUTH_RESP(5)、AUTH_FAIL(6)、ACK(7)
- 服务端自动兼容 V1（11字节头部）和 V2（15字节头部）协议

**新增功能：**
- 设备鉴权：HMAC-MD5 + Redis(Lettuce异步) + AuthHandler(鉴权后自动移除)
- 连接管理：DeviceChannelManager（踢旧保新、定向推送、在线统计）
- 消息确认：PendingAckManager（Caffeine TTL 30s，最大10万条）
- TLS 加密：独立端口(9001) + SslHandler + 自签名证书兜底
- 可观测性：Micrometer + Prometheus，9项核心指标
- 消息路由：MessageDispatcher + MessageHandler 接口，Spring Bean 自动注册
- 断线重连：客户端指数退避重连(1s→30s)，重连后自动重鉴权

**新增依赖：**
- spring-boot-starter-data-redis（Lettuce 异步客户端）
- caffeine（本地缓存）
- spring-kafka（消息持久化，P2 阶段启用）
- spring-boot-starter-actuator + micrometer-registry-prometheus（可观测性）

**新增配置项：**
- `netty.server.tls-enabled` / `tls-port` / `tls-cert-path` / `tls-cert-password`
- `netty.server.auth-timeout-seconds`
- `spring.data.redis.*`
- `management.endpoints.web.exposure.include`

### V1.0

- 自定义协议编解码（魔数 0x44565352 "DVSR"，11字节头部）
- 多协议接入探测器（自定义协议 / HTTP / MQTT）
- 心跳检测（5秒读空闲，3次断开）
- Spring Boot 集成（配置外化、生命周期管理）
- 交互式测试客户端 + 压力测试客户端
