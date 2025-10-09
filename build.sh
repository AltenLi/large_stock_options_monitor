#!/bin/bash

# V2多市场期权监控系统 Docker构建脚本

set -e

echo "=========================================="
echo "V2多市场期权监控系统 Docker构建"
echo "=========================================="

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo "❌ Docker未安装，请先安装Docker"
    exit 1
fi

# 检查docker-compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 设置镜像标签
IMAGE_NAME="v2-options-monitor"
IMAGE_TAG=${1:-latest}
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

echo "构建镜像: ${FULL_IMAGE_NAME}"

# 创建必要的目录
echo "创建必要的目录..."
mkdir -p docker v2_system/data v2_system/logs config

# 检查配置文件
if [ ! -f "v2_system/config.py" ]; then
    echo "配置文件不存在，从模板复制..."
    cp v2_system/config.py.sample v2_system/config.py
fi

# 构建Docker镜像
echo "开始构建Docker镜像..."
docker build -t ${FULL_IMAGE_NAME} .

if [ $? -eq 0 ]; then
    echo "✅ Docker镜像构建成功: ${FULL_IMAGE_NAME}"
    
    # 显示镜像信息
    echo ""
    echo "镜像信息:"
    docker images ${IMAGE_NAME}
    
    echo ""
    echo "🚀 构建完成！使用以下命令启动服务:"
    echo "   docker-compose up -d"
    echo ""
    echo "📖 更多信息请查看: docker/README.md"
else
    echo "❌ Docker镜像构建失败"
    exit 1
fi