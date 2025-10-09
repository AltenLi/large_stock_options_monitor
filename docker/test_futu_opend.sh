#!/bin/bash

# FutuOpenD 测试脚本

echo "=========================================="
echo "FutuOpenD 配置测试"
echo "=========================================="

# 检查环境变量
echo "1. 检查环境变量..."
echo "FUTU_LOGIN_ACCOUNT: ${FUTU_LOGIN_ACCOUNT:-未设置}"
echo "FUTU_LOGIN_PWD_MD5: ${FUTU_LOGIN_PWD_MD5:-未设置}"

# 检查文件权限
echo ""
echo "2. 检查文件权限..."
if [ -x "/app/futu_opend/FutuOpenD" ]; then
    echo "✅ FutuOpenD 可执行文件权限正常"
else
    echo "❌ FutuOpenD 可执行文件权限异常"
fi

# 检查配置文件
echo ""
echo "3. 检查配置文件..."
if [ -f "/app/futu_opend/FutuOpenD.xml" ]; then
    echo "✅ 配置文件存在"
    echo "当前配置的登录账号:"
    grep -o '<login_account>[^<]*</login_account>' /app/futu_opend/FutuOpenD.xml || echo "未找到登录账号配置"
else
    echo "❌ 配置文件不存在"
fi

# 检查端口
echo ""
echo "4. 检查端口占用..."
if command -v netstat >/dev/null 2>&1; then
    if netstat -ln | grep -q ":11111"; then
        echo "⚠️  端口 11111 已被占用"
    else
        echo "✅ 端口 11111 可用"
    fi
else
    echo "ℹ️  无法检查端口状态（netstat 不可用）"
fi

# 检查进程状态（如果在容器内运行）
echo ""
echo "5. 检查进程状态..."
if command -v supervisorctl >/dev/null 2>&1; then
    echo "Supervisor 进程状态:"
    supervisorctl status futu_opend 2>/dev/null || echo "FutuOpenD 进程未在 supervisor 中运行"
else
    echo "ℹ️  Supervisor 不可用"
fi

echo ""
echo "=========================================="
echo "测试完成"
echo "=========================================="