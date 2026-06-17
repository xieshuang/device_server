#!/bin/bash
# ============================================================
# Git Pre-Commit Hook: 提交前运行测试告警检查
# 安装: cp pre-commit-check.sh .git/hooks/pre-commit
#        chmod +x .git/hooks/pre-commit
# ============================================================
set -e

echo "Running pre-commit test alert check..."
bash scripts/test-alert.sh 50 /dev/null

EXIT=$?
if [ $EXIT -ne 0 ]; then
    echo ""
    echo "Test alert check FAILED! Use --no-verify to skip."
    exit 1
fi
exit 0