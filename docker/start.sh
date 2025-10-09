#!/bin/bash

# V2多市场期权监控系统启动脚本

set -e

echo "=========================================="
echo "V2多市场期权监控系统 Docker容器启动"
echo "=========================================="

# 激活conda环境
source /opt/conda/etc/profile.d/conda.sh
conda activate options_monitor

# 设置工作目录
cd /app/v2_system

# 检查配置文件
if [ ! -f "config.py" ]; then
    echo "配置文件不存在，从模板复制..."
    cp config.py.sample config.py
fi

# 如果存在外部配置文件，则使用外部配置
if [ -f "/app/config/config.py" ]; then
    echo "使用外部配置文件..."
    cp /app/config/config.py ./config.py
fi

# 配置 FutuOpenD
echo "配置 FutuOpenD..."
bash /app/docker/configure_futu_opend.sh

# 创建必要目录
mkdir -p data logs data/cache

# 检查富途API连接（可选）
echo "检查Python环境和依赖..."
python -c "
import sys
print(f'Python版本: {sys.version}')
try:
    import futu
    import pandas
    import flask
    print('✅ 核心依赖检查通过')
except ImportError as e:
    print(f'❌ 依赖检查失败: {e}')
    sys.exit(1)
"

# 启动supervisor管理进程
echo "启动进程管理器..."
exec supervisord -c /etc/supervisor/conf.d/options_monitor.conf -n