# 配置加密指南

## 1. 安装 Jasypt 依赖

已在 `pom.xml` 中引入 `jasypt-spring-boot-starter:3.0.5`。

## 2. 生成加密密文

```bash
# 设置加密密钥(不要提交到Git!)
export JASYPT_ENCRYPTOR_PASSWORD=your-master-key

# 加密明文
mvn jasypt:encrypt-value -Djasypt.encryptor.password=$JASYPT_ENCRYPTOR_PASSWORD \
    -Djasypt.plugin.value="your-redis-password"

# 输出: ENC(encrypted-string)
```

## 3. 替换配置文件

将 `application.yml` 中的明文密码替换为 ENC 密文：

```yaml
spring:
  data:
    redis:
      password: ENC(Km7xZ...)  # 替换明文密码

  kafka:
    properties:
      sasl.jaas.config: ENC(Ab3y...)  # Kafka SASL 密码
```

## 4. 启动时注入密钥

```bash
# 方式1: 环境变量
export JASYPT_ENCRYPTOR_PASSWORD=your-master-key
java -jar target/device-server.jar

# 方式2: 启动参数
java -jar target/device-server.jar -Djasypt.encryptor.password=your-master-key

# 方式3: Kubernetes Secret
kubectl create secret generic jasypt-key --from-literal=password=your-master-key
# 在 Deployment 中引用:
# env:
#   - name: JASYPT_ENCRYPTOR_PASSWORD
#     valueFrom:
#       secretKeyRef:
#         name: jasypt-key
#         key: password
```

## 5. 安全注意事项

- 密钥**绝不**提交到 Git 仓库
- 生产环境密钥通过 K8s Secret / Vault / 环境变量注入
- 密钥轮换：重新加密所有配置值 + 滚动重启服务
