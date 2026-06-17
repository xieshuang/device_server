#!/bin/bash
# ============================================================
# Device Server 测试异常告警脚本
# 用法: ./test-alert.sh [coverage-threshold] [alert-file]
#   告警条件: 测试失败 OR 覆盖率低于阈值
# ============================================================
set -euo pipefail

THRESHOLD="${1:-50}"
ALERT_FILE="${2:-TEST-ALERT.log}"
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
CSV_FILE="$PROJECT_DIR/target/site/jacoco/jacoco.csv"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ALERTS=()
PASS=true

log() { echo -e "$1"; }
alert() { ALERTS+=("$1"); PASS=false; log "${RED}[ALERT]${NC} $1"; }

echo "============================================================"
echo " Device Server 测试异常告警检查"
echo " 时间: $TIMESTAMP | 覆盖率阈值: ${THRESHOLD}%"
echo "============================================================"

# ===== 1. 运行测试 =====
echo ""
echo "[1/3] running tests..."

# 查找可用的 Maven 命令
if command -v mvn &>/dev/null; then
    MVN="mvn"
elif [ -f "D:/apache-maven-3.8.1/bin/mvn.cmd" ]; then
    MVN="D:/apache-maven-3.8.1/bin/mvn.cmd"
elif [ -f "/c/Program Files/Apache/maven/bin/mvn" ]; then
    MVN="/c/Program Files/Apache/maven/bin/mvn"
else
    echo "[ALERT] Maven not found"
    exit 1
fi

TEST_OUT=$("$MVN" test -f "$PROJECT_DIR/pom.xml" 2>&1)
TEST_EXIT=$?

if [ $TEST_EXIT -ne 0 ]; then
    # 提取失败测试
    FAILED_COUNT=$(echo "$TEST_OUT" | grep -oP 'Failures: \K\d+' | tail -1 || echo "?")
    ERROR_COUNT=$(echo "$TEST_OUT" | grep -oP 'Errors: \K\d+' | tail -1 || echo "?")
    alert "测试失败! Failures=$FAILED_COUNT, Errors=$ERROR_COUNT"

    # 提取失败详情
    echo "$TEST_OUT" | grep -A2 "<<< FAILURE" | while read -r line; do
        if echo "$line" | grep -q "FAILURE"; then
            alert "  └ $line"
        fi
    done
else
    log "${GREEN}[PASS]${NC} 所有测试通过"
fi

# ===== 2. 检查覆盖率 =====
echo ""
echo "[2/3] checking coverage..."

if [ ! -f "$CSV_FILE" ]; then
    alert "JaCoCo 报告不存在 ($CSV_FILE)"
else
    COV_LINE=$(python3 -c "
import csv
with open('$CSV_FILE') as f:
    rows = [r for r in csv.DictReader(f) if 'protobuf' not in r['PACKAGE'] and 'client' not in r['PACKAGE']]
cov = sum(int(r['LINE_COVERED']) for r in rows)
miss = sum(int(r['LINE_MISSED']) for r in rows)
rate = round(cov / (cov + miss) * 100, 1) if (cov + miss) > 0 else 0
zero = sum(1 for r in rows if int(r['LINE_COVERED']) == 0 and int(r['LINE_MISSED']) > 0)
print(f'{rate} {zero}')
" 2>/dev/null)

    if [ -n "$COV_LINE" ]; then
        COV=$(echo "$COV_LINE" | awk '{print $1}')
        ZERO=$(echo "$COV_LINE" | awk '{print $2}')

        if (( $(echo "$COV < $THRESHOLD" | bc -l) )); then
            alert "覆盖率 $COV% 低于阈值 ${THRESHOLD}% (差距: $(echo "$THRESHOLD - $COV" | bc)%)"
        else
            log "${GREEN}[PASS]${NC} 覆盖率 $COV% ≥ ${THRESHOLD}%"
        fi

        if [ "$ZERO" -gt 0 ]; then
            log "${YELLOW}[WARN]${NC} $ZERO 个类零覆盖"
        fi
    fi
fi

# ===== 3. 检查测试数量趋势 =====
echo ""
echo "[3/3] checking test counts..."

TEST_COUNT=$(echo "$TEST_OUT" | grep -oP 'Tests run: \K\d+' | tail -1 || echo "0")
if [ "$TEST_COUNT" == "0" ] || [ -z "$TEST_COUNT" ]; then
    alert "测试计数异常: $TEST_COUNT"
else
    log "${GREEN}[INFO]${NC} 测试用例: $TEST_COUNT"
fi

# ===== 4. 生成告警日志 =====
echo ""
echo "============================================================"
if $PASS; then
    log "${GREEN}✓ 所有检查通过${NC}"
else
    log "${RED}✗ ${#ALERTS[@]} 项告警${NC}"
    echo ""
    echo "=== 告警报告 ($TIMESTAMP) ===" > "$ALERT_FILE"
    echo "共 ${#ALERTS[@]} 项告警:" >> "$ALERT_FILE"
    for a in "${ALERTS[@]}"; do
        echo "  - $a" >> "$ALERT_FILE"
    done
    echo "" >> "$ALERT_FILE"
    echo "报告已保存: $ALERT_FILE"
    cat "$ALERT_FILE"
    exit 1
fi
echo "============================================================"
exit 0