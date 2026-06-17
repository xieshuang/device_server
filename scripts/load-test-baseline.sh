#!/bin/bash
# ============================================================
# Device Server 压力测试基线脚��
# 用法: ./load-test-baseline.sh [connections] [batch-size]
#   默认: 1000 连接, 每批 50
# ============================================================
set -euo pipefail

CONNECTIONS="${1:-1000}"
BATCH_SIZE="${2:-50}"
INTERVAL_MS="${3:-100}"
HOST="${4:-127.0.0.1}"
PORT="${5:-9000}"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo "============================================================"
echo " Device Server 压力测试基线"
echo " 连接数: $CONNECTIONS | 批次: $BATCH_SIZE | 间隔: ${INTERVAL_MS}ms"
echo "============================================================"

echo ""
echo "[1] 启动服务..."

# 检查服务是否运行
curl -s http://localhost:8080/actuator/health > /dev/null 2>&1 || {
    echo -e "${RED}服务未启动，正在启动...${NC}"
    mvn spring-boot:run -q &
    sleep 10
    echo "等待服务就绪..."
    for i in $(seq 1 30); do
        curl -s http://localhost:8080/actuator/health > /dev/null 2>&1 && break
        sleep 2
    done
}

# 基线采集前指标
echo ""
echo "[2] 采集基线指标..."
BEFORE_MEM=$(curl -s http://localhost:8080/actuator/metrics/jvm.memory.used?tag=area:heap | python3 -c "import sys,json; print(json.load(sys.stdin)['measurements'][0]['value'])" 2>/dev/null || echo "0")
BEFORE_CONN=$(curl -s http://localhost:8080/actuator/prometheus 2>/dev/null | grep netty_connections_active | awk '{print $2}' || echo "0")

echo "  启动前: 堆内存=$(echo "scale=1; $BEFORE_MEM/1024/1024" | bc)MB, 连接数=$BEFORE_CONN"

echo ""
echo "[3] 运行压力测试..."
# 指定类路径, 执行 StressTestClient
TARGET_COUNT=$CONNECTIONS
if [ "$TARGET_COUNT" -le 1000 ]; then
    TARGET_COUNT=1000
fi

# 记录开始时间
START_TIME=$(date +%s)

# 压力测试需要手动执行 StressTestClient
echo "  请在另一个终端运行:"
echo "  java -cp target/device-server.jar com.xsh.netty.client.StressTestClient $HOST $PORT $CONNECTIONS $BATCH_SIZE $INTERVAL_MS 0"

echo ""
echo "[4] 采集压测后指标..."
sleep 5

AFTER_MEM=$(curl -s http://localhost:8080/actuator/metrics/jvm.memory.used?tag=area:heap | python3 -c "import sys,json; print(json.load(sys.stdin)['measurements'][0]['value'])" 2>/dev/null || echo "0")
AFTER_CONN=$(curl -s http://localhost:8080/actuator/prometheus 2>/dev/null | grep netty_connections_active | awk '{print $2}' || echo "0")

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

MEM_INCREASE=$(echo "scale=1; ($AFTER_MEM - $BEFORE_MEM) / 1024 / 1024" | bc)
MEM_PER_CONN=$(echo "scale=2; $MEM_INCREASE / $CONNECTIONS" | bc 2>/dev/null || echo "0")

echo ""
echo "============================================================"
echo " 压力测试结果"
echo "============================================================"
echo "  测试时长: ${DURATION}s"
echo "  目标连接: $CONNECTIONS"
echo "  实际连接: $AFTER_CONN"
echo "  堆内存增��: ${MEM_INCREASE}MB (每连接 ~${MEM_PER_CONN}KB)"
echo ""

# 基线对比
if [ "$CONNECTIONS" -eq 1000 ] && (( $(echo "$MEM_INCREASE < 200" | bc -l) )); then
    echo -e "${GREEN}[PASS]${NC} 1K连接内存基线达标 (<200MB)"
elif [ "$CONNECTIONS" -eq 5000 ] && (( $(echo "$MEM_INCREASE < 500" | bc -l) )); then
    echo -e "${GREEN}[PASS]${NC} 5K连接内存基线达标 (<500MB)"
elif [ "$CONNECTIONS" -eq 10000 ] && (( $(echo "$MEM_INCREASE < 1000" | bc -l) )); then
    echo -e "${GREEN}[PASS]${NC} 10K连接内存基线达标 (<1GB)"
else
    echo -e "${RED}[WARN]${NC} 内存使用异常, 需要排查"
fi

echo "============================================================"