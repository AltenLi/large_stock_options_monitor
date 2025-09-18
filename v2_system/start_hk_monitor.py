#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
港股期权监控启动脚本
专门用于启动港股期权大单监控
"""

import sys
import os
import time
import logging
from datetime import datetime

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from option_monitor_v2 import V2OptionMonitor
from config import (
    HK_MONITOR_STOCKS, 
    is_hk_trading_time,
    get_market_type,
    should_monitor_market,
    should_update_data_off_hours,
    FUTU_CONFIG
)

# 为了兼容性，创建别名
HK_STOCK_CODES = HK_MONITOR_STOCKS

def setup_logging():
    """设置日志"""
    log_dir = os.path.join(current_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'hk_monitor_{datetime.now().strftime("%Y%m%d")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def main():
    """主函数"""
    logger = setup_logging()
    logger.info("🇭🇰 港股期权监控系统启动")
    
    try:
        # 检查港股交易时间和配置开关
        is_trading = is_hk_trading_time()
        should_monitor = should_monitor_market('HK')
        allow_off_hours = should_update_data_off_hours('HK')
        
        if is_trading:
            logger.info("✅ 当前为港股交易时间，系统将正常监控并发送所有通知")
        elif should_monitor:
            logger.warning("⏰ 当前非港股交易时间，但调试开关已开启，系统将继续监控数据但不发送额外通知")
        else:
            logger.warning("🔒 当前非港股交易时间且调试开关已关闭，系统将不进行数据更新")
            logger.info("💡 如需在非交易时间调试，请在config.py中设置 HK_TRADING_HOURS['update_data_off_hours'] = True")
            return
        
        # 显示监控的港股列表
        logger.info(f"📊 监控港股列表: {HK_STOCK_CODES}")
        for stock_code in HK_STOCK_CODES:
            market_type = get_market_type(stock_code)
            logger.info(f"  - {stock_code} ({market_type}市场)")
        
        logger.info(f"🔧 调试配置: 非开市时间更新数据 = {allow_off_hours}")
        
        # 创建监控实例
        monitor = V2OptionMonitor(market='HK')
        
        # 设置监控股票列表（通过配置传入）
        logger.info(f"📋 设置港股监控列表: {len(HK_STOCK_CODES)} 只股票")
        
        logger.info("🚀 开始港股期权监控...")
        
        # 开始监控
        monitor.start_monitoring()
        
    except KeyboardInterrupt:
        logger.info("👋 用户中断，港股期权监控系统退出")
    except Exception as e:
        logger.error(f"❌ 港股期权监控系统异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        logger.info("🔚 港股期权监控系统结束")

if __name__ == "__main__":
    main()
