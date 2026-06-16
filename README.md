# Device Server - 工业级 Netty 多协议设备接入服务器

基于 Spring Boot 3 + Netty 构建的工业级多协议设备接入服务器，支持自定义协议、HTTP、MQTT 三种协议接入，内置心跳检测机制。

---

## 1. 技术栈

| 组件 | 版本 | 说明 |
|------|------|------|
| Java | 17 | LTS 版本 |
| Spring Boot | 3.2.5 | 依赖注入、配置管理、生命周期 |
| Netty | 4.1.109.Final | 网络通信框架 |
| Jackson | Spring Boot 内置 | JSON 序列化 |
| Lombok | Spring Boot 内置 | 减少样板代码 |

---

## 2. 架构设计

### 2.1 整体架构

```
[设备/客户端] ──TCP──> [Netty Server]
                          │
                    [IdleStateHandler]     ← 空闲检测
                          │
                    [MultiProtocolDetector] ← 协议嗅探，动态路由
                          │
               ┌──────────┼──────────┐
               ▼          ▼          ▼
        [CustomDecoder] [HttpCodec] [MqttDecoder]
               │          │          │
        [CustomHandler] [HttpHandler] [MqttHandler]
               │          │          │
               └──────────┼──────────┘
                          │
                   [businessGroup]      ← 独立线程池，不阻塞 I/O
```

### 2.2 线程模型

```
bossGroup (1线程)        → 接收新连接（Acceptor）
workerGroup (CPU×2线程)   → I/O 读写（EventLoop）
businessGroup (64线程)    → 业务处理（独立线程池，防阻塞 I/O）
```

### 2.3 自定义协议帧结构

```
+------------+--------+-----------+---------+-----------+---------+
| Magic(4B)  | Ver(1B)| Serial(1B)| Type(1B)| Length(4B)| Body(N) |
+------------+--------+-----------+---------+-----------+---------+
| 0x44565352 |   1    |  1=JSON   | 1/2/3   |  Body长度  | 变长数据 |
|   "DVSR"   |        | 2=Protobuf|         |           |         |
+------------+--------+-----------+---------+-----------+---------+
```

| 字段 | 长度 | 说明 |
|------|------|------|
| Magic Number | 4 字节 | 协议标识 `0x44565352` ("DVSR") |
| Version | 1 字节 | 协议版本号，默认 1 |
| Serialization Type | 1 字节 | 序列化方式：1=JSON，2=Protobuf(预留) |
| Msg Type | 1 字节 | 消息类型：1=心跳请求，2=心跳响应，3=业务数据 |
| Length | 4 字节 | Body 的字节长度 |
| Body | 变长 | 实际数据（心跳为字符串，业务走序列化） |

固定头部共 **11 字节**。

### 2.4 多协议探测机制

`MultiProtocolDetector` 位于 Pipeline 最前端，通过嗅探数据包前几个字节判断协议类型，然后动态替换自身为对应的编解码器：

| 协议 | 嗅探规则 | 动态插入的 Handler |
|------|----------|-------------------|
| 自定义协议 | 魔数 `0x44565352` ("DVSR") | CustomProtocolDecoder + CustomProtocolEncoder |
| HTTP | 首字母匹配 GET/POST/PUT/DELETE/HEAD/PATCH/OPTIONS | HttpServerCodec + HttpObjectAggregator |
| MQTT | 首字节高4位=1 (CONNECT 报文) | MqttDecoder + MqttEncoder |
| 未知 | 不匹配以上规则 | 直接关闭连接 |

### 2.5 心跳检测机制

```
IdleStateHandler(5s读空闲) → 触发 IdleStateEvent → CustomProtocolHandler.userEventTriggered()
                                                         │
                                                   空闲计数器 +1
                                                         │
                                              count >= 3 ? ──→ 关闭连接
                                                   │
                                              count < 3  ──→ 等待下次检测
```

- **5秒** 无读数据触发空闲事件
- 连续 **3次** 空闲才断开，避免网络瞬时抖动造成误杀
- 收到任何数据（含心跳响应）立即重置计数器

### 2.6 Epoll 自动切换

```java
boolean useEpoll = Epoll.isAvailable();
// Linux → EpollEventLoopGroup + EpollServerSocketChannel（性能更优）
// 其他  → NioEventLoopGroup + NioServerSocketChannel
```

---

## 3. 项目结构

```
src/main/java/com/xsh/netty/
├── DeviceServerApplication.java          # Spring Boot 主启动类
├── protocol/
│   ├── MessageHeader.java                # 协议头定义（魔数、版本、类型等）
│   ├── MessagePacket.java                # 消息包（头部 + Body）
│   └── MsgType.java                      # 消息类型常量
├── serialize/
│   ├── Serializer.java                   # 序列化接口
│   └── JsonSerializer.java               # JSON 序列化实现
├── codec/
│   ├── CustomProtocolEncoder.java        # 自定义协议编码器
│   ├── CustomProtocolDecoder.java        # 自定义协议解码器（粘包半包 + 帧长度校验）
│   └── MultiProtocolDetector.java        # 多协议探测器（动态路由）
├── handler/
│   ├── CustomProtocolHandler.java        # 自定义协议业务处理器（心跳 + 业务）
│   ├── HttpBusinessHandler.java          # HTTP 业务处理器
│   └── MqttBusinessHandler.java          # MQTT 业务处理器
├── server/
│   └── NettyServerProperties.java        # 配置属性类
├── config/
│   └── NettyServerBootstrap.java         # 服务启动引导（生命周期管理）
└── client/
    ├── TestClient.java                   # 交互式测试客户端
    └── StressTestClient.java             # 压力测试客户端
```

---

## 4. 配置说明

`application.yml` 完整配置：

```yaml
server:
  port: 8080                    # Spring Boot HTTP 端口（管理用）

netty:
  server:
    port: 9000                  # Netty TCP 监听端口
    boss-threads: 1             # Acceptor 线程数（1个足够）
    worker-threads: 0           # I/O 线程数（0=默认 CPU核心数×2）
    business-threads: 64        # 业务处理线程池大小
    idle-timeout-seconds: 5     # 读空闲超时（秒）
    max-idle-count: 3           # 最大连续空闲次数
    max-frame-length: 10485760  # 单帧最大长度（10MB），防 OOM
    so-backlog: 1024            # TCP 连接排队数
```

---

## 5. 安全防护

| 防护点 | 实现方式 |
|--------|----------|
| 非法协议 | 魔数校验失败直接关闭连接 |
| 超大帧攻击 | `maxFrameLength` 上限校验（10MB），超过关闭连接 |
| 负数长度 | length < 0 直接关闭连接 |
| 恶意扫描 | 未知协议直接关闭连接 |
| I/O 阻塞 | 业务 Handler 在独立 `businessGroup` 线程池执行 |
| 内存泄漏 | Netty 内置 ByteBuf 自动释放；异常路径统一 `ctx.close()` |

---

## 6. 测试方案

### 6.1 单元测试

已实现的单元测试：

| 测试类 | 测试项 | 数量 |
|--------|--------|------|
| `CustomProtocolCodecTest` | 心跳编解码、业务编解码、非法魔数、超大帧、半包处理 | 5 |
| `NettyServerPropertiesTest` | 默认配置值校验 | 1 |

运行方式：

```bash
mvn test
```

### 6.2 交互式测试

使用 `TestClient` 进行手动验证：

```bash
# 默认连接 127.0.0.1:9000
java com.xsh.netty.client.TestClient

# 指定地址和端口
java com.xsh.netty.client.TestClient 192.168.1.100 9000
```

操作方式：
- 连接建立后自动发送心跳 PING
- 控制台输入文本回车发送业务数据
- 输入 `quit` 退出

### 6.3 十六进制报文测试

使用网络调试工具（NetAssist、PacketSender 等）发送十六进制报文：

**心跳请求 "PING"：**
```
44 56 53 52   # Magic: "DVSR"
01            # Version: 1
01            # Serialization: JSON
01            # MsgType: 心跳请求
00 00 00 04   # Length: 4
50 49 4E 47   # Body: "PING"
```

**业务数据 "hello"：**
```
44 56 53 52   # Magic
01            # Version
01            # Serialization: JSON
03            # MsgType: 业务数据
00 00 00 07   # Length: 7
22 68 65 6C 6C 6F 22   # Body: "hello" (JSON 序列化后带引号)
```

### 6.4 多协议测试

| 协议 | 推荐工具 | 说明 |
|------|----------|------|
| 自定义协议 | TestClient / 网络调试助手 | 发送十六进制报文 |
| HTTP | Postman / curl | 发送 GET/POST 请求到 9000 端口 |
| MQTT | MQTTX | 连接 9000 端口，发送 CONNECT 报文 |

### 6.5 压力测试

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
# 调高文件描述符上限
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

## 7. 快速启动

### 7.1 编译

```bash
mvn clean package -DskipTests
```

### 7.2 启动服务端

```bash
java -jar target/device-server-1.0.0-SNAPSHOT.jar
```

启动成功后日志输出：
```
Netty server started on port: 9000 (Epoll: false)
```

### 7.3 启动测试客户端

```bash
# 交互式测试
java -cp target/device-server-1.0.0-SNAPSHOT.jar com.xsh.netty.client.TestClient

# 压力测试
java -cp target/device-server-1.0.0-SNAPSHOT.jar com.xsh.netty.client.StressTestClient
```

---

## 8. 生产环境注意事项

1. **Linux 部署启用 Epoll**：自动检测，需确保系统安装 `libnetty-transport-native-epoll`
2. **内存泄漏防护**：业务代码中手工创建的 `ByteBuf` 必须遵循 `ReferenceCountUtil.release(msg)`
3. **业务隔离**：禁止在 I/O 线程中做数据库查询、RPC 调用、复杂计算，必须提交到 `businessGroup`
4. **帧长度限制**：`maxFrameLength` 根据实际业务调整，防止恶意客户端导致 OOM
5. **日志级别**：生产环境建议将 Netty 相关日志设为 `WARN`，避免大量心跳日志
