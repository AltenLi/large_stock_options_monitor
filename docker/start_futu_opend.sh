#!/bin/bash

# FutuOpenD 启动包装脚本

set -e

# 设置默认值
FUTU_LOGIN_ACCOUNT=${FUTU_LOGIN_ACCOUNT:-100000}
FUTU_LOGIN_PWD_MD5=${FUTU_LOGIN_PWD_MD5:-123456}

echo "启动 FutuOpenD..."
echo "登录账号: $FUTU_LOGIN_ACCOUNT"

# 切换到 FutuOpenD 目录
cd /app/futu_opend

# 启动 FutuOpenD
exec ./FutuOpenD -login_account=$FUTU_LOGIN_ACCOUNT -login_pwd_md5=$FUTU_LOGIN_PWD_MD5 -lang=chs