# V5.x 后续版本演进规划

> 制定日期：2026-06-17  
> 当前版本：v5.0.0（75 测试，行覆盖 31.7%）

---

## 一、现状回顾

| 维度 | 完成度 | 说明 |
|------|--------|------|
| 协议覆盖 | 100% | 6 协议（DVSR/Modbus/OPC-UA/HTTP/WS/MQTT 探测） |
| 生产基础设施 | 100% | ACK 时间轮/背压/IP 过滤/优雅停机/集群/限流 |
| 安全性 | 90% | HMAC + nonce 防重放 + 设备吊销 + TLS |
| 测试覆盖（行） | 31.7% | 75 用例，编解码层 75% 达标 |
| 业务 Handler | 30% | 零测试：CustomProtocol/HTTP/MQTT |
| 运维 API | 0% | 无 REST 管理接口 |

---

## 二、版本路线图

```
V5.1              V5.2                V5.3
测试补齐+运维API  协议完备+物模型V2  安全增强+生产就绪
(5天)             (4天)               (2天)
```

---

## 三、V5.1 — 测试补齐 + 运维 API（5 天）

**目标**：行覆盖 31.7% → 55%

### 阶段 A：P0 测试补齐（3 天）

| 新增测试类 | 未覆盖行 | 内容 |
|-----------|---------|------|
| `CustomProtocolHandlerTest` | 62 | 心跳/ACK/Dispatcher/空闲检测 |
| `RedisAuthServiceTest` | 43 | HMAC 校验/时间戳/吊销/Nonce |
| `AuthHandlerTest`（扩展） | 76 | 时间戳偏差/设备未注册/异常路径 |
| `NettyMetricsBinderTest` | 53 | 12 项指标注册验证 |

### 阶段 B：安全测试（1 天）

| 新增测试类 | 内容 |
|-----------|------|
| `NonceValidatorTest` | SETNX 成功/重复/Redis 异常 |
| `DeviceRevocationServiceTest` | 吊销/解除/查询 |
| `RateLimitHandlerTest` | 心跳豁免/限流丢弃/关闭 |
| `ClusterSessionManagerTest` | Lua 注册/注销/令牌校验 |

### 阶段 C：运维 REST API（1 天）

3 个 Controller，通过 Actuator 8080 端口暴露：

| Controller | 端点 | 功能 |
|-----------|------|------|
| `DeviceManagementController` | `/api/devices/*` | 在线列表/强制下线/设备吊销/统计 |
| `FirewallController` | `/api/firewall/*` | 黑名单查询/手动封禁/解除 |
| `RateLimitController` | `/api/ratelimit/*` | 全局/单设备限流查询与动态调整 |

---

## 四、V5.2 — 协议完备 + 物模型 V2（4 天）

**目标**：MQTT 3.1.1 + 动态脚本引擎

### 阶段 A：MQTT 完整协议（2 天）

| 报文 | 当前 | 目标 |
|------|------|------|
| CONNECT/CONNACK | 仅嗅探 | 握手 + username/password 鉴权 |
| SUBSCRIBE/SUBACK | 未实现 | Topic 订阅 + QoS 协商 |
| PUBLISH/PUBACK | 未实现 | QoS 0/1 发布 |
| PINGREQ/PINGRESP | 未实现 | 心跳保活 |
| DISCONNECT | 未实现 | 优雅断开 + 遗嘱消息 |

### 阶段 B：动态脚本引擎（2 天）

`ScriptEngineMessageHandler`：从 Redis 加载 JavaScript 脚本，GraalVM Polyglot 执行。
- 脚本超时 5s，内存限制 10MB
- 禁止文件 IO/网络/系统调用
- 脚本 MD5 签名防篡改

---

## 五、V5.3 — 安全增强 + 生产就绪（2 天）

### 阶段 A：运维增强（1 天）

- 全量配置 Jasypt 加密
- Grafana 告警规则（鉴权失败率/ACK积压/Kafka失败率）
- 消息大小 Histogram 监控

### 阶段 B：压力测试基线（1 天）

| 指标 | 目标 |
|------|------|
| 10K 连接内存 | < 500MB |
| 单消息延迟 P99 | < 50ms |
| Kafka 投递延迟 P99 | < 100ms |

---

## 六、里程碑

| 版本 | 覆盖率 | 核心交付 | 预估 |
|------|--------|---------|------|
| V5.1 | 55% | P0 测试补齐 + 6 个 REST 端点 | 5 天 |
| V5.2 | 60% | MQTT 完整协议 + 动态脚本 | 4 天 |
| V5.3 | 65% | 配置加密 + 告警 + 压测基线 | 2 天 |

**总预估**：11 天达到生产级完整度。

---

> 建议从 V5.1 阶段 A（P0 Handler 测试补齐）开始实施。
