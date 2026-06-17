# Device Server - 工业级 Netty 多协议设备接入服务器

基于 Spring Boot 3 + Netty 构建的工业级多协议设备接入服务器，支持自定义协议、HTTP、MQTT、WebSocket 四种协议接入，内置设备鉴权、心跳检测、消息确认、TLS 加密、Kafka 持久化、流量控制等工业级特性。

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
| Guava | 33.0.0-jre | 令牌桶限流（RateLimiter） |
| Protobuf | 3.25.3 | 二进制序列化（高性能，与 JSON 并存） |
| Micrometer + Prometheus | Spring Boot 内置 | 可观测性指标暴露 |
| Lombok | Spring Boot 内置 | 减少样板代码 |

---

## 2. 架构设计

### 2.1 整体架构

```
[设备/客户端] ──TCP/TLS──> [Netty Server]
                               │
                         [SslHandler]          ← TLS 加密（可选，独立端口 9001）
                               │
                         [ReadTimeoutHandler]   ← 鉴权超时检测
                               │
                         [IdleStateHandler]     ← 空闲检测
                               │
                         [MultiProtocolDetector] ← 协议嗅探，动态路由
                               │
                    ┌──────────┼──────────┬──────────┐
                    ▼          ▼          ▼          ▼
             [CustomDecoder] [HttpCodec] [MqttDecoder] [WebSocket升级]
                    │          │          │              │
             [AuthHandler]    │          │         [WSHandler]
                    │          │          │              │
             [RateLimitHandler]│         │              │
                    │          │          │              │
             [CustomHandler] [HttpHandler] [MqttHandler] [WSBizHandler]
                    │                                        │
             [MessageDispatcher] ←──────────────────────────┘
                    │
             [BusinessMessageHandler] → [Kafka]
                    │
             [businessGroup]     ← 独立线程池，不阻塞 I/O
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

---

## 3. 项目结构

```
src/main/java/com/xsh/netty/
├── DeviceServerApplication.java          # Spring Boot 主启动类
├── protocol/
│   ├── MessageHeader.java                # 协议头定义（V1/V2 兼容，含 sequenceId）
│   ├── MessagePacket.java                # 消息包（头部 + Body + rawBody）
│   ├── MsgType.java                      # 消息类型常量（8种）
│   ├── AuthRequest.java                  # 鉴权请求体（deviceId + timestamp + token）
│   ├── ChannelAttributes.java            # Channel 属性键（deviceId, authenticated, negotiatedVersion）
│   ├── VersionInfo.java                  # 协议版本常量（SERVER_MAX/MIN_VERSION）
│   └── VersionNegotiator.java            # 版本协商器（min(clientVersion, SERVER_MAX_VERSION)）
├── serialize/
│   ├── Serializer.java                   # 序列化接口（JSON=1, Protobuf=2）
│   ├── JsonSerializer.java               # JSON 序列化实现
│   └── ProtobufSerializer.java           # Protobuf 序列化实现（Class→Parser 动态映射）
├── codec/
│   ├── CustomProtocolEncoder.java        # V2 协议编码器
│   ├── CustomProtocolDecoder.java        # V1/V2 自适应解码器（粘包半包 + 帧长度校验）
│   ├── MultiProtocolDetector.java        # 多协议探测器（自定义/HTTP/WS/MQTT 动态路由）
│   └── WebSocketFrameCodec.java          # WebSocket 帧 ↔ MessagePacket 转换器
├── auth/
│   ├── AuthService.java                  # 鉴权服务接口
│   ├── HmacUtils.java                    # HMAC-MD5 工具类
│   └── RedisAuthService.java             # Redis 异步鉴权实现
├── handler/
│   ├── AuthHandler.java                  # 鉴权处理器（鉴权+版本协商后自动移除）
│   ├── CustomProtocolHandler.java        # 自定义协议业务处理器（心跳 + ACK + Dispatcher）
│   ├── BusinessMessageHandler.java       # 业务消息处理器（Kafka 持久化 + 指标记录）
│   ├── WebSocketBusinessHandler.java     # WebSocket 业务处理器（复用 Dispatcher）
│   ├── HttpBusinessHandler.java          # HTTP 业务处理器
│   └── MqttBusinessHandler.java          # MQTT 业务处理器
├── server/
│   ├── NettyServerProperties.java        # 配置属性类（含 TLS/Kafka/限流/WebSocket）
│   ├── DeviceChannelManager.java         # 设备连接管理器（踢旧保新 + 定向推送）
│   ├── DeviceSession.java                # 设备会话信息
│   └── PendingAckManager.java            # 待确认消息管理器（Caffeine TTL）
├── dispatcher/
│   ├── MessageHandler.java               # 业务消息处理器接口
│   └── MessageDispatcher.java            # 消息分发器（按 msgType 路由）
├── ratelimit/
│   ├── RateLimiterService.java           # 限流服务（全局+单设备双维度令牌桶）
│   └── RateLimitHandler.java             # Pipeline 限流 Handler
├── kafka/
│   ├── KafkaProducerService.java         # Kafka 异步发送服务
│   ├── KafkaProducerConfig.java          # Kafka Producer 配置
│   └── KafkaMessageEnvelope.java         # 消息信封（headers + payload + receivedAt）
├── config/
│   ├── NettyServerBootstrap.java         # 服务启动引导（TLS + 鉴权 + 限流 + WS + 优雅停机）
│   ├── NettyMetricsBinder.java           # Micrometer 指标注册（含 Kafka/限流/WS 指标）
│   ├── DispatcherConfig.java             # 消息分发器配置（Spring Bean 自动注册）
│   └── HandlerBeanContainer.java         # Handler 依赖容器（解决 Netty Handler Bean 注入）
└── client/
    ├── TestClient.java                   # 交互式测试客户端（鉴权 + 断线重连）
    └── StressTestClient.java             # 压力测试客户端（万级连接）
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
12. **maven-enforcer-plugin**：构建时检测重复依赖声明

---

## 10. 演进路线

| 阶段 | 状态 | 内容 |
|------|------|------|
| V1.0 基础框架 | ✅ 已完成 | 编解码、多协议接入、心跳检测、Spring Boot 集成 |
| P0 生产必须 | ✅ 已完成 | 设备鉴权(Redis+HMAC)、连接管理(踢旧保新)、消息确认(Caffeine TTL)、TLS 加密 |
| P1 运维必备 | ✅ 已完成 | 可观测性(Micrometer+Prometheus)、消息路由分发、断线重连(指数退避) |
| P2 规模化 | ✅ 已完成 | 流量控制(令牌桶)、消息持久化(Kafka)、协议版本协商 |
| P3 扩展功能 | ✅ 已完成 | WebSocket 支持、Protobuf 序列化、Grafana 看板（MQTT 暂不接入） |

---

## 11. 版本变更记录

### V3.1 (当前) — 生产级安全与健壮性修复

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
