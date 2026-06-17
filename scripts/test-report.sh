#!/bin/bash
# ============================================================
# Device Server 自动化测试报告生成脚本
# 用法: ./test-report.sh [mode]
#   mode: quick  - 仅运行测试（默认）
#         full   - clean + test + 覆盖率报告
#         report - 仅解析已有覆盖率数据生成报告
# ============================================================
set -euo pipefail

MODE="${1:-quick}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
REPORT_DIR="$PROJECT_DIR/target/site/jacoco"
CSV_FILE="$REPORT_DIR/jacoco.csv"
OUTPUT_FILE="$PROJECT_DIR/AUTO-TEST-REPORT.md"
MVN_CMD="./mvnw"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# 如果没有 mvnw，尝试使用系统 mvn
if [ ! -f "$PROJECT_DIR/mvnw" ]; then
    MVN_CMD="mvn"
fi

echo "============================================================"
echo " Device Server 自动化测试报告"
echo " 时间: $TIMESTAMP"
echo " 模式: $MODE"
echo "============================================================"

# ===== 运行测试 =====
if [ "$MODE" == "full" ]; then
    echo ""
    echo "[1/3] cleaning..."
    $MVN_CMD clean -q -f "$PROJECT_DIR/pom.xml"

    echo "[2/3] running tests with coverage..."
    $MVN_CMD test -f "$PROJECT_DIR/pom.xml" 2>&1 | tail -5
elif [ "$MODE" == "quick" ]; then
    echo ""
    echo "[1/2] running tests with coverage..."
    $MVN_CMD test -f "$PROJECT_DIR/pom.xml" 2>&1 | tail -5
fi

# ===== 解析覆盖率 =====
echo ""
echo "[3/3] parsing coverage data..."

if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR: JaCoCo CSV not found at $CSV_FILE"
    echo "Make sure jacoco-maven-plugin is configured in pom.xml"
    exit 1
fi

# Python 解析
python3 - << 'PYEOF'
import csv
import sys
from collections import defaultdict

csv_file = sys.argv[1] if len(sys.argv) > 1 else "target/site/jacoco/jacoco.csv"

total = {"class": 0, "line_cov": 0, "line_miss": 0,
         "branch_cov": 0, "branch_miss": 0, "method_cov": 0, "method_miss": 0}
pkg_stats = defaultdict(lambda: {"line_cov": 0, "line_miss": 0})
top = []
zero = []

with open(csv_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        pkg = row['PACKAGE']
        if 'protobuf' in pkg or 'generated' in pkg or 'client' in pkg:
            continue

        lc = int(row['LINE_COVERED'])
        lm = int(row['LINE_MISSED'])
        total_ln = lc + lm

        total["class"] += 1
        total["line_cov"] += lc
        total["line_miss"] += lm
        total["branch_cov"] += int(row['BRANCH_COVERED'])
        total["branch_miss"] += int(row['BRANCH_MISSED'])
        total["method_cov"] += int(row['METHOD_COVERED'])
        total["method_miss"] += int(row['METHOD_MISSED'])

        pkg_name = ".".join(pkg.split(".")[-2:]) if "." in pkg else pkg
        pkg_stats[pkg_name]["line_cov"] += lc
        pkg_stats[pkg_name]["line_miss"] += lm

        if total_ln > 0:
            rate = lc / total_ln * 100
            cls = row['CLASS'].split('.')[-1]
            if rate >= 80:
                top.append((pkg_name, cls, lc, total_ln, rate))
            if lc == 0 and lm > 0 and not cls.startswith("DeviceServerApplication"):
                zero.append((pkg_name, cls, lm))

# 计算总体比例
lt = total["line_cov"] + total["line_miss"]
bt = total["branch_cov"] + total["branch_miss"]
mt = total["method_cov"] + total["method_miss"]
line_rate = total["line_cov"] / lt * 100 if lt > 0 else 0
branch_rate = total["branch_cov"] / bt * 100 if bt > 0 else 0
method_rate = total["method_cov"] / mt * 100 if mt > 0 else 0

# 级别
if line_rate >= 80: grade = "🟢 A"
elif line_rate >= 60: grade = "🟡 B"
elif line_rate >= 40: grade = "🟠 C"
elif line_rate >= 20: grade = "🔴 D"
else: grade = "⚫ E"

print(f"TOTAL_CLASSES={total['class']}")
print(f"LINE_COV={total['line_cov']}")
print(f"LINE_MISS={total['line_miss']}")
print(f"LINE_RATE={line_rate:.1f}")
print(f"BRANCH_COV={total['branch_cov']}")
print(f"BRANCH_MISS={total['branch_miss']}")
print(f"BRANCH_RATE={branch_rate:.1f}")
print(f"METHOD_COV={total['method_cov']}")
print(f"METHOD_MISS={total['method_miss']}")
print(f"METHOD_RATE={method_rate:.1f}")
print(f"GRADE={grade}")
print(f"TOP_COUNT={len(top)}")
print(f"ZERO_COUNT={len(zero)}")

# 包级统计
print("PKG_STATS_START")
for pkg in sorted(pkg_stats.keys()):
    s = pkg_stats[pkg]
    s_lt = s["line_cov"] + s["line_miss"]
    s_rate = s["line_cov"] / s_lt * 100 if s_lt > 0 else 0
    bar_len = int(s_rate / 5)
    bar = "█" * bar_len + "░" * (20 - bar_len)
    print(f"PKG|{pkg}|{s['line_cov']}|{s['line_miss']}|{s_rate:.0f}|{bar}")
print("PKG_STATS_END")

print("TOP_START")
for pkg, cls, lc, total, rate in sorted(top, key=lambda x: -x[4]):
    print(f"TOP|{pkg}.{cls}|{lc}/{total}|{rate:.0f}%")
print("TOP_END")

print("ZERO_START")
for pkg, cls, lm in zero:
    print(f"ZERO|{pkg}.{cls}|0/{lm}")
print("ZERO_END")
PYEOF

echo ""
echo "Report generation complete."
echo "HTML report: $REPORT_DIR/index.html"
echo "Markdown report: $OUTPUT_FILE"