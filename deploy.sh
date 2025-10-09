#!/bin/bash

# V2多市场期权监控系统 Docker部署脚本

set -e

echo "=========================================="
echo "V2多市场期权监控系统 Docker部署"
echo "=========================================="

# 函数：显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  start     启动服务"
    echo "  stop      停止服务"
    echo "  restart   重启服务"
    echo "  status    查看状态"
    echo "  logs      查看日志"
    echo "  build     构建镜像"
    echo "  clean     清理资源"
    echo "  help      显示帮助"
    echo ""
}

# 函数：检查环境
check_environment() {
    echo "检查环境..."
    
    # 检查Docker
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker未安装"
        exit 1
    fi
    
    # 检查Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose未安装"
        exit 1
    fi
    
    # 检查Docker服务状态
    if ! docker info &> /dev/null; then
        echo "❌ Docker服务未运行"
        exit 1
    fi
    
    echo "✅ 环境检查通过"
}

# 函数：准备配置
prepare_config() {
    echo "准备配置..."
    
    # 创建必要目录
    mkdir -p v2_system/data v2_system/logs config
    
    # 检查环境变量文件
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            echo "⚠️  .env文件不存在，从模板复制..."
            cp .env.example .env
            echo "📝 请编辑 .env 文件配置相关参数"
        else
            echo "⚠️  环境变量文件不存在，将使用默认配置"
        fi
    fi
    
    # 检查配置文件
    if [ ! -f "v2_system/config.py" ]; then
        echo "配置文件不存在，从模板复制..."
        cp v2_system/config.py.sample v2_system/config.py
    fi
    
    echo "✅ 配置准备完成"
}

# 函数：启动服务
start_service() {
    echo "启动服务..."
    check_environment
    prepare_config
    
    # 构建镜像（如果不存在）
    if ! docker images | grep -q "v2-options-monitor"; then
        echo "镜像不存在，开始构建..."
        docker-compose build
    fi
    
    # 启动服务
    docker-compose up -d
    
    echo "✅ 服务启动成功"
    echo ""
    echo "🌐 Web界面: http://localhost:5001"
    echo "📊 查看状态: $0 status"
    echo "📝 查看日志: $0 logs"
}

# 函数：停止服务
stop_service() {
    echo "停止服务..."
    docker-compose down
    echo "✅ 服务已停止"
}

# 函数：重启服务
restart_service() {
    echo "重启服务..."
    docker-compose restart
    echo "✅ 服务已重启"
}

# 函数：查看状态
show_status() {
    echo "服务状态:"
    docker-compose ps
    
    echo ""
    echo "容器健康状态:"
    docker-compose exec options-monitor supervisorctl status 2>/dev/null || echo "无法连接到容器"
}

# 函数：查看日志
show_logs() {
    echo "查看日志 (按Ctrl+C退出):"
    docker-compose logs -f
}

# 函数：构建镜像
build_image() {
    echo "构建镜像..."
    check_environment
    docker-compose build --no-cache
    echo "✅ 镜像构建完成"
}

# 函数：清理资源
clean_resources() {
    echo "⚠️  这将删除所有容器、镜像和网络"
    read -p "确认继续? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "清理资源..."
        docker-compose down -v --rmi all --remove-orphans
        echo "✅ 资源清理完成"
    else
        echo "取消清理"
    fi
}

# 主逻辑
case "${1:-help}" in
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        restart_service
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    build)
        build_image
        ;;
    clean)
        clean_resources
        ;;
    help|*)
        show_help
        ;;
esac