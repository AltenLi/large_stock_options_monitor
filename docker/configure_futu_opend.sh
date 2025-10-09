#!/bin/bash

# FutuOpenD 配置脚本
# 根据环境变量动态配置 FutuOpenD.xml

set -e

FUTU_CONFIG_FILE="/app/futu_opend/FutuOpenD.xml"
FUTU_CONFIG_TEMPLATE="/app/futu_opend/FutuOpenD.xml.template"

echo "配置 FutuOpenD..."

# 设置默认值
FUTU_LOGIN_ACCOUNT=${FUTU_LOGIN_ACCOUNT:-100000}
FUTU_LOGIN_PWD_MD5=${FUTU_LOGIN_PWD_MD5:-123456}

echo "登录账号: $FUTU_LOGIN_ACCOUNT"

# 如果存在模板文件，使用模板；否则备份原文件作为模板
if [ ! -f "$FUTU_CONFIG_TEMPLATE" ]; then
    echo "创建配置模板..."
    cp "$FUTU_CONFIG_FILE" "$FUTU_CONFIG_TEMPLATE"
fi

# 使用 sed 替换配置文件中的占位符
sed -e "s|<login_account>100000</login_account>|<login_account>$FUTU_LOGIN_ACCOUNT</login_account>|g" \
    -e "s|<login_pwd>123456</login_pwd>|<login_pwd>$FUTU_LOGIN_PWD_MD5</login_pwd>|g" \
    "$FUTU_CONFIG_TEMPLATE" > "$FUTU_CONFIG_FILE"

echo "FutuOpenD 配置完成"
echo "配置文件位置: $FUTU_CONFIG_FILE"