#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多市场期权监控启动脚本
同时支持港股和美股期权大单监控
"""

import sys
import os
import time
import logging
import threading
from datetime import datetime
from typing import List, Optional

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from option_monitor_v2 import V2OptionMonitor
from config import (
    HK_MONITOR_STOCKS, 
    US_MONITOR_STOCKS,
    is_hk_trading_time,
    is_us_trading_time,
    get_market_type,
    should_monitor_market,
    should_update_data_off_hours,
    FUTU_CONFIG
)

def setup_logging():
    """设置日志"""
    log_dir = os.path.join(current_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'multi_market_{datetime.now().strftime("%Y%m%d")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

class MultiMarketMonitor:
    """多市场期权监控器"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.hk_monitor = None
        self.us_monitor = None
        self.running = False
        
        # 批次轮流请求控制
        self.api_semaphore = threading.Semaphore(1)  # 同时只允许一个市场请求API
        self.market_turn_lock = threading.Lock()  # 市场轮流锁
        self.current_turn = 'HK'  # 当前轮到的市场 ('HK' 或 'US')
        self.active_markets = set()  # 活跃的市场集合
        self.last_api_call = {}  # 每个市场的上次API调用时间
        self.min_api_interval = 5  # API调用最小间隔(秒)
        
        # 监控配置
        self.hk_enabled = len(HK_MONITOR_STOCKS) > 0 and should_update_data_off_hours('HK')
        self.us_enabled = len(US_MONITOR_STOCKS) > 0 and should_update_data_off_hours('US')
        
        self.logger.info(f"监控配置 - 港股: {'启用' if self.hk_enabled else '禁用'}, 美股: {'启用' if self.us_enabled else '禁用'}")
        if len(HK_MONITOR_STOCKS) > 0 and not should_update_data_off_hours('HK'):
            self.logger.info("港股监控已禁用：非交易时间调试开关已关闭")
        if len(US_MONITOR_STOCKS) > 0 and not should_update_data_off_hours('US'):
            self.logger.info("美股监控已禁用：非交易时间调试开关已关闭")
        
    def register_market(self, market: str):
        """注册活跃市场"""
        with self.market_turn_lock:
            self.active_markets.add(market)
            # 如果是第一个注册的市场，设为当前轮次
            if len(self.active_markets) == 1:
                self.current_turn = market
                self.logger.info(f"市场 {market} 已注册为首个市场，设为当前轮次")
            else:
                self.logger.info(f"市场 {market} 已注册，当前活跃市场: {self.active_markets}")
    
    def unregister_market(self, market: str):
        """注销市场"""
        with self.market_turn_lock:
            self.active_markets.discard(market)
            self.logger.info(f"市场 {market} 已注销，当前活跃市场: {self.active_markets}")
    
    def wait_for_turn_and_acquire_api(self, market: str) -> bool:
        """等待轮到该市场并获取API访问权限"""
        # 如果只有一个市场活跃，直接获取API权限
        with self.market_turn_lock:
            if len(self.active_markets) <= 1:
                self.logger.debug(f"{market} 市场：单一市场模式，直接获取API权限")
                self.api_semaphore.acquire()
                return True
        
        # 多市场模式：等待轮到该市场
        max_wait_cycles = 60  # 最多等待60个周期（约5分钟）
        wait_cycle = 0
        
        while self.running and wait_cycle < max_wait_cycles:
            with self.market_turn_lock:
                if self.current_turn == market:
                    # 轮到该市场，尝试获取API权限
                    if self.api_semaphore.acquire(blocking=False):
                        self.logger.info(f"✅ {market} 市场获得API访问权限")
                        return True
                    else:
                        self.logger.warning(f"⚠️ {market} 市场轮到但API被占用")
                        
            # 等待5秒后重试
            time.sleep(5)
            wait_cycle += 1
        
        self.logger.error(f"❌ {market} 市场等待API权限超时")
        return False
    
    def release_api_and_switch_turn(self, market: str):
        """释放API权限并切换到下一个市场"""
        try:
            # 记录API调用时间
            self.last_api_call[market] = time.time()
            
            # 释放API权限
            self.api_semaphore.release()
            
            # 切换到下一个市场
            with self.market_turn_lock:
                if len(self.active_markets) > 1:
                    # 多市场模式：切换到另一个市场
                    if market == 'HK' and 'US' in self.active_markets:
                        self.current_turn = 'US'
                    elif market == 'US' and 'HK' in self.active_markets:
                        self.current_turn = 'HK'
                    
                    self.logger.info(f"🔄 API权限已释放，下一轮: {self.current_turn}")
                else:
                    # 单市场模式：保持当前市场
                    self.logger.debug(f"🔄 {market} 市场：单一市场模式，API权限已释放")
                    
        except Exception as e:
            self.logger.error(f"释放API权限时出错: {e}")
    
    def wait_for_api_cooldown(self, market: str):
        """等待API冷却时间"""
        if market in self.last_api_call:
            elapsed = time.time() - self.last_api_call[market]
            if elapsed < self.min_api_interval:
                wait_time = self.min_api_interval - elapsed
                self.logger.debug(f"{market} API冷却：等待{wait_time:.1f}秒")
                time.sleep(wait_time)
    
    def start_hk_monitor(self):
        """启动港股监控"""
        if not self.hk_enabled:
            self.logger.info("🇭🇰 港股监控已禁用（无监控股票）")
            return
            
        try:
            self.logger.info("🇭🇰 启动港股期权监控线程")
            self.hk_monitor = V2OptionMonitor(market='HK')
            self.logger.info(f"📋 港股监控列表: {len(HK_MONITOR_STOCKS)} 只股票")
            
            # 注册港股市场
            self.register_market('HK')
            
            # 监控循环 - 根据市场数量调整间隔
            if self.hk_enabled and self.us_enabled:
                scan_interval = 120  # 多市场模式：2分钟
            else:
                scan_interval = 60   # 单市场模式：1分钟
            
            while self.running:
                try:
                    is_trading = is_hk_trading_time()
                    should_monitor = should_monitor_market('HK')
                    
                    if is_trading or should_monitor:
                        self.logger.info("🇭🇰 港股监控准备扫描...")
                        
                        # 等待轮到港股并获取API权限
                        if self.wait_for_turn_and_acquire_api('HK'):
                            try:
                                # 等待API冷却
                                self.wait_for_api_cooldown('HK')
                                
                                if is_trading:
                                    self.logger.info("✅ 港股交易时间，正常监控并发送所有通知")
                                    self.hk_monitor.manual_scan()
                                else:
                                    self.logger.info("⏰ 港股非交易时间，继续监控数据但不发送额外通知")
                                    self.hk_monitor.manual_scan()
                                    
                            finally:
                                # 释放API权限并切换轮次
                                self.release_api_and_switch_turn('HK')
                        else:
                            self.logger.warning("⚠️ 港股监控未能获取API权限，跳过本次扫描")
                    else:
                        self.logger.info("🔒 港股非交易时间且调试开关已关闭，跳过数据更新")
                        # 港股跳过时，需要切换API权限给其他市场
                        if len(self.active_markets) > 1:
                            with self.market_turn_lock:
                                if self.current_turn == 'HK':
                                    if 'US' in self.active_markets:
                                        self.current_turn = 'US'
                                        self.logger.info("🔄 港股跳过扫描，切换API权限给美股")
                    
                    # 等待下次扫描
                    self.logger.info(f"港股监控等待{scan_interval}秒(约{scan_interval/60:.1f}分钟)后下次扫描")
                    time.sleep(scan_interval)
                    
                except Exception as e:
                    self.logger.error(f"❌ 港股监控异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟
                    
        except Exception as e:
            self.logger.error(f"❌ 港股监控线程异常: {e}")
        finally:
            # 注销港股市场
            self.unregister_market('HK')
    
    def start_us_monitor(self):
        """启动美股监控"""
        if not self.us_enabled:
            self.logger.info("🇺🇸 美股监控已禁用（无监控股票）")
            return
            
        try:
            self.logger.info("🇺🇸 启动美股期权监控线程")
            self.us_monitor = V2OptionMonitor(market='US')
            self.logger.info(f"📋 美股监控列表: {len(US_MONITOR_STOCKS)} 只股票")
            
            # 注册美股市场
            self.register_market('US')
            
            # 如果是多市场模式，美股线程等待60秒错峰启动
            if self.hk_enabled and self.us_enabled:
                self.logger.info("美股监控线程等待60秒，错峰启动...")
                time.sleep(60)
            
            # 监控循环 - 根据市场数量调整间隔
            if self.hk_enabled and self.us_enabled:
                scan_interval = 120  # 多市场模式：2分钟
            else:
                scan_interval = 60   # 单市场模式：1分钟
            
            while self.running:
                try:
                    is_trading = is_us_trading_time()
                    should_monitor = should_monitor_market('US')
                    
                    if is_trading or should_monitor:
                        self.logger.info("🇺🇸 美股监控准备扫描...")
                        
                        # 等待轮到美股并获取API权限
                        if self.wait_for_turn_and_acquire_api('US'):
                            try:
                                # 等待API冷却
                                self.wait_for_api_cooldown('US')
                                
                                if is_trading:
                                    self.logger.info("✅ 美股交易时间，正常监控并发送所有通知")
                                    self.us_monitor.manual_scan()
                                else:
                                    self.logger.info("⏰ 美股非交易时间，继续监控数据但不发送额外通知")
                                    self.us_monitor.manual_scan()
                                    
                            finally:
                                # 释放API权限并切换轮次
                                self.release_api_and_switch_turn('US')
                        else:
                            self.logger.warning("⚠️ 美股监控未能获取API权限，跳过本次扫描")
                    else:
                        self.logger.info("🔒 美股非交易时间且调试开关已关闭，跳过数据更新")
                        # 美股跳过时，需要切换API权限给其他市场
                        if len(self.active_markets) > 1:
                            with self.market_turn_lock:
                                if self.current_turn == 'US':
                                    if 'HK' in self.active_markets:
                                        self.current_turn = 'HK'
                                        self.logger.info("🔄 美股跳过扫描，切换API权限给港股")
                    
                    # 等待下次扫描
                    self.logger.info(f"美股监控等待{scan_interval}秒(约{scan_interval/60:.1f}分钟)后下次扫描")
                    time.sleep(scan_interval)
                    
                except Exception as e:
                    self.logger.error(f"❌ 美股监控异常: {e}")
                    time.sleep(60)  # 异常时等待1分钟
                    
        except Exception as e:
            self.logger.error(f"❌ 美股监控线程异常: {e}")
        finally:
            # 注销美股市场
            self.unregister_market('US')
    
    def start_monitoring(self):
        """开始多市场监控"""
        self.running = True
        
        # 根据配置决定启动哪些线程
        threads = []
        
        if self.hk_enabled:
            hk_thread = threading.Thread(target=self.start_hk_monitor, name="HK-Monitor")
            hk_thread.daemon = True
            threads.append(('HK', hk_thread))
        
        if self.us_enabled:
            us_thread = threading.Thread(target=self.start_us_monitor, name="US-Monitor")
            us_thread.daemon = True
            threads.append(('US', us_thread))
        
        if not threads:
            self.logger.error("❌ 没有启用任何市场监控，请检查配置")
            return
        
        # 启动线程
        for market, thread in threads:
            self.logger.info(f"🚀 启动{market}监控线程...")
            thread.start()
        
        # 显示启动模式
        if len(threads) == 1:
            self.logger.info("📱 单一市场监控模式：无需等待API轮次")
        else:
            self.logger.info("🔄 多市场监控模式：批次轮流请求API")
        
        self.logger.info("🚀 多市场期权监控已启动")
        
        try:
            # 主线程保持运行
            while self.running:
                # 每10分钟输出一次状态
                time.sleep(600)
                
                status_info = []
                for market, thread in threads:
                    status = "运行中" if thread.is_alive() else "已停止"
                    status_info.append(f"{market}: {status}")
                
                self.logger.info(f"📊 监控状态 - {', '.join(status_info)}")
                
                # 检查线程是否还活着，如果死了就重启
                for i, (market, thread) in enumerate(threads):
                    if not thread.is_alive():
                        self.logger.warning(f"🔄 {market}监控线程已停止，重新启动...")
                        if market == 'HK':
                            new_thread = threading.Thread(target=self.start_hk_monitor, name="HK-Monitor")
                        else:
                            new_thread = threading.Thread(target=self.start_us_monitor, name="US-Monitor")
                        new_thread.daemon = True
                        new_thread.start()
                        threads[i] = (market, new_thread)
                    
        except KeyboardInterrupt:
            self.logger.info("👋 用户中断，停止监控")
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """停止监控"""
        self.running = False
        self.logger.info("🛑 多市场期权监控已停止")

def main():
    """主函数"""
    logger = setup_logging()
    logger.info("🌍 多市场期权监控系统启动")
    
    try:
        # 显示监控配置
        logger.info("📊 监控配置:")
        hk_enabled = len(HK_MONITOR_STOCKS) > 0
        us_enabled = len(US_MONITOR_STOCKS) > 0
        
        if hk_enabled:
            logger.info(f"  🇭🇰 港股: {len(HK_MONITOR_STOCKS)} 只股票")
            for stock_code in HK_MONITOR_STOCKS:
                logger.info(f"    - {stock_code}")
        else:
            logger.info("  🇭🇰 港股: 已禁用（无监控股票）")
        
        if us_enabled:
            logger.info(f"  🇺🇸 美股: {len(US_MONITOR_STOCKS)} 只股票")
            for stock_code in US_MONITOR_STOCKS:
                logger.info(f"    - {stock_code}")
        else:
            logger.info("  🇺🇸 美股: 已禁用（无监控股票）")
        
        # 显示运行模式
        if hk_enabled and us_enabled:
            logger.info("🔄 多市场模式：批次轮流请求API，避免并发冲突")
            logger.info("⏱️ 轮询间隔: 2分钟/市场，市场间自动轮流")
        elif hk_enabled or us_enabled:
            logger.info("📱 单一市场模式：无需等待API轮次，直接请求")
            logger.info("⏱️ 轮询间隔: 1分钟（单市场优化）")
        else:
            logger.error("❌ 没有启用任何市场监控，请检查配置")
            return
        
        # 检查当前交易时间
        if hk_enabled:
            hk_trading = is_hk_trading_time()
            logger.info(f"  🇭🇰 港股: {'交易中' if hk_trading else '休市'}")
        
        if us_enabled:
            us_trading = is_us_trading_time()
            logger.info(f"  🇺🇸 美股: {'交易中' if us_trading else '休市'}")
        
        # 创建并启动多市场监控
        monitor = MultiMarketMonitor()
        monitor.start_monitoring()
        
    except KeyboardInterrupt:
        logger.info("👋 用户中断，多市场期权监控系统退出")
    except Exception as e:
        logger.error(f"❌ 多市场期权监控系统异常: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        logger.info("🔚 多市场期权监控系统结束")

if __name__ == "__main__":
    main()