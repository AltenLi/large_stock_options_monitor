#!/bin/bash

# V2多市场期权监控系统健康检查脚本

# 检查Web服务
check_web_service() {
    local url="http://localhost:5001"
    local timeout=10
    
    if curl -f -s --max-time $timeout "$url" > /dev/null 2>&1; then
        echo "✅ Web服务正常"
        return 0
    else
        echo "❌ Web服务异常"
        return 1
    fi
}

# 检查监控进程
check_monitor_process() {
    if pgrep -f "start_multi_market_monitor.py" > /dev/null; then
        echo "✅ 监控进程正常"
        return 0
    else
        echo "❌ 监控进程异常"
        return 1
    fi
}

# 检查数据库文件
check_database() {
    local hk_db="/app/v2_system/data/hk_options_monitor_v2.db"
    local us_db="/app/v2_system/data/us_options_monitor_v2.db"
    
    if [ -f "$hk_db" ] || [ -f "$us_db" ]; then
        echo "✅ 数据库文件存在"
        return 0
    else
        echo "⚠️  数据库文件不存在（首次运行正常）"
        return 0
    fi
}

# 检查日志文件
check_logs() {
    local log_dir="/app/v2_system/logs"
    
    if [ -d "$log_dir" ] && [ "$(ls -A $log_dir 2>/dev/null)" ]; then
        echo "✅ 日志文件正常"
        return 0
    else
        echo "⚠️  日志目录为空"
        return 0
    fi
}

# 主健康检查
main() {
    echo "开始健康检查..."
    
    local exit_code=0
    
    check_web_service || exit_code=1
    check_monitor_process || exit_code=1
    check_database
    check_logs
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ 系统健康检查通过"
    else
        echo "❌ 系统健康检查失败"
    fi
    
    exit $exit_code
}

main "$@"