# -*- coding: utf-8 -*-
"""
V2系统港股期权大单监控主程序 - 完全独立版本
"""

import time
import logging
import traceback
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import threading
import signal
import sys
import os
import argparse

# 添加V2系统路径 - 支持从根目录启动
current_dir = os.path.dirname(os.path.abspath(__file__))
v2_system_dir = current_dir if current_dir.endswith('v2_system') else os.path.join(current_dir, 'v2_system')
sys.path.insert(0, v2_system_dir)

# 设置工作目录为v2_system
if not os.getcwd().endswith('v2_system'):
    v2_work_dir = v2_system_dir
    os.chdir(v2_work_dir)
    print(f"V2系统工作目录已切换到: {v2_work_dir}")

# 第三方库
try:
    import futu as ft
    import json
except ImportError as e:
    print(f"请安装必要的依赖包: {e}")
    print("pip install futu-api pandas numpy scipy flask requests")
    sys.exit(1)

from config import *
from utils.logger import setup_logger
from utils.notifier import V2Notifier
from utils.data_handler import V2DataHandler
from utils.mac_notifier import MacNotifier
from utils.big_options_processor import BigOptionsProcessor


class V2OptionMonitor:
    """V2系统多市场期权大单监控器"""
    
    def __init__(self, market: str = 'HK'):
        self.market = market
        self.logger = setup_logger(f'V2OptionMonitor.{market}')
        self.notifier = V2Notifier()
        self.data_handler = V2DataHandler(market)
        self.mac_notifier = MacNotifier()
        self.big_options_processor = BigOptionsProcessor(market)
        self.quote_ctx = None
        self.is_running = False
        self.monitor_thread = None
        self.connection_thread = None
        self.polling_thread = None
        self.subscribed_options = set()  # 已订阅的期权代码
        self.stock_price_cache = {}  # 股价缓存
        self.price_update_time = {}  # 股价更新时间
        self.option_chain_cache = {}  # 期权链缓存
        self.last_scan_time = None
        self.scan_count = 0
        self.previous_options = {}  # 上次扫描的期权数据
        
        # 连接状态管理
        self.connection_lock = threading.Lock()
        self.is_connected = False
        self.connection_retry_count = 0
        self.max_retry_count = 5
        
        self.logger.info("V2系统期权监控器初始化完成")
    
    def _maintain_connection(self):
        """后台线程维持与OpenD的持久连接"""
        self.logger.info("V2系统连接维护线程启动")
        
        while self.is_running:
            try:
                # 只在连接状态为False时才尝试重连，避免过度检查
                with self.connection_lock:
                    if not self.is_connected:
                        self.logger.info("V2系统检测到连接断开，尝试重新连接...")
                        if self._connect_futu_internal():
                            self.is_connected = True
                            self.connection_retry_count = 0
                            self.logger.info("V2系统连接恢复成功")
                        else:
                            self.is_connected = False
                            self.connection_retry_count += 1
                            self.logger.warning(f"V2系统连接失败，重试次数: {self.connection_retry_count}")
                            
                            if self.connection_retry_count >= self.max_retry_count:
                                self.logger.error("V2系统连接重试次数超限，停止监控")
                                self.is_running = False
                                break
                
                # 延长检查间隔到2分钟，减少不必要的连接测试
                time.sleep(120)
                
            except Exception as e:
                self.logger.error(f"V2系统连接维护线程异常: {e}")
                time.sleep(60)
        
        self.logger.info("V2系统连接维护线程退出")
    
    def _connect_futu_internal(self) -> bool:
        """内部连接方法（不加锁）"""
        try:
            # 如果已有连接，直接使用，不进行额外测试
            if self.quote_ctx:
                self.logger.debug("V2系统使用现有连接")
                return True
            
            # 建立新连接
            self.logger.info("V2系统建立新的富途连接...")
            self.quote_ctx = ft.OpenQuoteContext(
                host=FUTU_CONFIG['host'], 
                port=FUTU_CONFIG['port']
            )
            return True
                
        except Exception as e:
            self.logger.warning(f"V2系统连接富途OpenD失败: {e}")
            # 异常时确保清理连接
            try:
                if self.quote_ctx:
                    self.quote_ctx.close()
                    self.quote_ctx = None
            except:
                pass
            return False
    
    def connect_futu(self, max_retries: int = 3, retry_delay: int = 5) -> bool:
        """连接富途OpenD（带重试机制）- 兼容性方法"""
        with self.connection_lock:
            if self._connect_futu_internal():
                self.is_connected = True
                return True
            else:
                self.is_connected = False
                return False
    
    def _polling_loop(self):
        """定时轮询线程 - 每2分钟轮询一次数据"""
        self.logger.info("V2系统数据轮询线程启动")
        
        while self.is_running:
            try:
                # 检查连接状态，但不过度测试
                if self.is_connected and self.quote_ctx:
                    try:
                        # 执行数据扫描
                        self.scan_big_options()
                    except Exception as scan_error:
                        self.logger.error(f"V2系统扫描异常: {scan_error}")
                        # 如果是连接相关错误，标记连接失效
                        if "连接" in str(scan_error) or "connection" in str(scan_error).lower():
                            with self.connection_lock:
                                self.is_connected = False
                                self.logger.warning("V2系统扫描时检测到连接问题，标记连接失效")
                else:
                    self.logger.warning("V2系统连接不可用，跳过本次轮询")
                
                # 等待2分钟
                for _ in range(120):  # 120秒 = 2分钟
                    if not self.is_running:
                        break
                    time.sleep(1)
                
            except Exception as e:
                self.logger.error(f"V2系统轮询线程异常: {e}")
                self.logger.error(traceback.format_exc())
                time.sleep(30)  # 异常后等待30秒再继续
        
        self.logger.info("V2系统数据轮询线程退出")
    
    def disconnect_futu(self):
        """断开富途连接"""
        try:
            if self.quote_ctx:
                self.quote_ctx.close()
                self.logger.info("V2系统富途OpenD连接已断开")
        except Exception as e:
            self.logger.error(f"V2系统断开富途连接失败: {e}")
    
    def get_stock_price(self, stock_code: str) -> float:
        """获取股票价格（带缓存）"""
        try:
            current_time = datetime.now()
            
            # 检查缓存
            if (stock_code in self.stock_price_cache and 
                stock_code in self.price_update_time and
                (current_time - self.price_update_time[stock_code]).seconds < 300):  # 5分钟缓存
                return self.stock_price_cache[stock_code]
            
            # 获取实时价格
            ret, data = self.quote_ctx.get_market_snapshot([stock_code])
            if ret == ft.RET_OK and not data.empty:
                price = float(data.iloc[0]['last_price'])
                self.stock_price_cache[stock_code] = price
                self.price_update_time[stock_code] = current_time
                return price
            else:
                # 使用默认价格
                return get_stock_default_price(stock_code)
                
        except Exception as e:
            self.logger.error(f"V2系统获取{stock_code}股价失败: {e}")
            return get_stock_default_price(stock_code)  # 使用config中的默认价格
    
    def scan_big_options(self) -> List[Dict]:
        """扫描大单期权"""
        try:
            self.scan_count += 1
            self.logger.info(f"V2系统开始第{self.scan_count}次大单期权扫描...")
            
            # 确保连接可用
            if not self.ensure_connection():
                self.logger.error("V2系统富途连接不可用，跳过本次扫描")
                return []
            
            # 第一次扫描时确保已加载历史数据
            if self.scan_count == 1 and not self.previous_options:
                self.logger.info("V2系统第一次扫描，加载历史数据进行diff比较")
                self.load_previous_options()
            
            # 根据市场选择对应的股票列表
            if self.market == 'HK':
                monitor_stocks = HK_MONITOR_STOCKS
                self.logger.info(f"V2系统港股监控，股票列表: {monitor_stocks}")
            elif self.market == 'US':
                monitor_stocks = US_MONITOR_STOCKS
                self.logger.info(f"V2系统美股监控，股票列表: {monitor_stocks}")
            else:
                self.logger.error(f"V2系统不支持的市场类型: {self.market}")
                return []
            
            # 获取大单期权
            big_options = self.big_options_processor.get_recent_big_options(
                self.quote_ctx, 
                monitor_stocks,
                option_monitor=self
            )
            
            if big_options:
                self.logger.info(f"V2系统发现 {len(big_options)} 笔大单期权")
                
                # 与历史数据比较，计算增量（在保存之前比较）
                big_options_with_diff = self.compare_with_previous_options(big_options)
                
                # 发送一次合并的汇总通知（包含所有有变化的股票）
                if big_options_with_diff:
                    # 使用V1风格的汇总报告，将所有股票合并在一个通知中
                    self.notifier.send_v1_style_summary_report(big_options_with_diff)
                
                # 保存数据（保存原始数据，确保下次比较时有正确的基准）
                self.data_handler.save_option_data(big_options)
                
                # 更新历史数据（按期权代码更新，保持全量缓存字典）
                if not hasattr(self, 'previous_options') or self.previous_options is None:
                    self.previous_options = {}
                
                # 将当前期权数据按代码更新到缓存字典中
                for current_opt in big_options:
                    option_code = current_opt.get('option_code', '')
                    if option_code:
                        self.previous_options[option_code] = current_opt
                
                self.logger.info(f"V2系统缓存更新: 当前{len(big_options)}个期权，全量缓存{len(self.previous_options)}个期权")
                
            else:
                self.logger.info("V2系统本次扫描未发现大单期权")
            
            self.last_scan_time = datetime.now()
            return big_options
            
        except Exception as e:
            self.logger.error(f"V2系统扫描大单期权失败: {e}")
            self.logger.error(traceback.format_exc())
            
            # 如果是连接相关错误，标记连接为无效
            if "连接" in str(e) or "connection" in str(e).lower():
                self.quote_ctx = None
            
            return []
    
    def monitor_loop(self):
        """监控主循环 - 已废弃，使用轮询线程架构"""
        self.logger.warning("V2系统monitor_loop方法已废弃，请使用_polling_loop轮询线程")
        return
    
    def is_trading_time(self) -> bool:
        """检查是否在交易时间（支持多市场）"""
        try:
            from config import is_market_trading_time
            
            # 检查港股或美股是否有任一市场在交易时间
            hk_trading = is_market_trading_time('HK')
            us_trading = is_market_trading_time('US')
            
            # 任一市场在交易时间就返回True
            return hk_trading or us_trading
            
        except Exception as e:
            self.logger.error(f"V2系统检查交易时间失败: {e}")
            return True  # 异常时默认为交易时间
    
    
    def start_monitoring(self):
        """启动监控"""
        try:
            if self.is_running:
                self.logger.warning("V2系统监控已在运行中")
                return
            
            # 连接富途
            if not self.connect_futu():
                self.logger.error("V2系统无法连接富途OpenD，监控启动失败")
                return
            
            # 加载历史数据作为比较基准
            self.load_previous_options()
            
            self.is_running = True
            
            # 启动后台线程
            # 启动连接维护线程
            self.connection_thread = threading.Thread(target=self._maintain_connection, daemon=True)
            self.connection_thread.start()
            
            # 启动数据轮询线程（替代旧的monitor_thread）
            self.polling_thread = threading.Thread(target=self._polling_loop, daemon=True)
            self.polling_thread.start()
            
            # 不再启动旧的monitor_thread，避免重复扫描
            self.monitor_thread = None
            
            self.logger.info("V2系统期权监控已启动（持久连接 + 2分钟轮询）")
            
            # 发送启动通知 - 加载历史数据，但不立即扫描（避免与轮询线程重复）
            self.logger.info("V2系统启动，加载历史数据")
            
            # 发送简单启动通知，实际扫描由轮询线程负责
            self.notifier.send_wework_notification("V2系统期权大单监控已启动")
            self.logger.info("V2系统启动通知已发送，轮询线程将开始监控")
            self.mac_notifier.send_notification("V2系统启动", "期权大单监控已开始运行")
            
        except Exception as e:
            self.logger.error(f"V2系统启动监控失败: {e}")
            self.logger.error(traceback.format_exc())
    
    def stop_monitoring(self):
        """停止监控"""
        try:
            if not self.is_running:
                self.logger.warning("V2系统监控未在运行")
                return
            
            self.is_running = False
            
            # 等待所有线程结束
            if self.connection_thread and self.connection_thread.is_alive():
                self.connection_thread.join(timeout=5)
            
            if self.polling_thread and self.polling_thread.is_alive():
                self.polling_thread.join(timeout=5)
            
            if self.monitor_thread and self.monitor_thread.is_alive():
                self.monitor_thread.join(timeout=5)
            
            # 断开富途连接
            self.disconnect_futu()
            
            self.logger.info("V2系统期权监控已停止")
            
            # 发送停止通知
            self.notifier.send_wework_notification("V2系统期权大单监控已停止")
            
        except Exception as e:
            self.logger.error(f"V2系统停止监控失败: {e}")
    
    def get_status(self) -> Dict:
        """获取监控状态"""
        return {
            'is_running': self.is_running,
            'last_scan_time': self.last_scan_time.isoformat() if self.last_scan_time else None,
            'scan_count': self.scan_count,
            'subscribed_options': len(self.subscribed_options),
            'cached_stocks': len(self.stock_price_cache),
            'is_trading_time': self.is_trading_time(),
            'system_version': 'V2'
        }
    
    def _send_consolidated_report(self, big_options_with_diff: List[Dict]):
        """发送合并的汇总报告 - 一次扫描只发送一次通知"""
        try:
            if not big_options_with_diff:
                return
            
            # 按股票分组统计
            stock_summary = {}
            total_trades = len(big_options_with_diff)
            total_amount = 0
            
            for option in big_options_with_diff:
                stock_code = option.get('stock_code', '')
                stock_name = option.get('stock_name', '')
                amount = option.get('turnover', 0)  # 使用turnover字段作为金额
                
                if stock_code not in stock_summary:
                    stock_summary[stock_code] = {
                        'name': stock_name,
                        'trades': 0,
                        'amount': 0,
                        'options': []
                    }
                
                stock_summary[stock_code]['trades'] += 1
                stock_summary[stock_code]['amount'] += amount
                stock_summary[stock_code]['options'].append(option)
                total_amount += amount
            
            # 生成汇总报告
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            report_lines = [
                f"[V2系统] 📊 期权监控汇总报告",
                f"⏰ 时间: {current_time}",
                f"📈 总交易: {total_trades} 笔",
                f"💰 总金额: {total_amount:,.0f} 港币",
                "",
                "📋 大单统计:"
            ]
            
            # 按金额排序股票
            sorted_stocks = sorted(stock_summary.items(), 
                                 key=lambda x: x[1]['amount'], reverse=True)
            
            for stock_code, info in sorted_stocks:
                report_lines.append(f"• {info['name']} ({stock_code}): {info['trades']}笔, {info['amount']:,.0f}港币")
                
                # 显示前3个最大的期权
                top_options = sorted(info['options'], 
                                   key=lambda x: x.get('amount', 0), reverse=True)[:3]
                
                for i, opt in enumerate(top_options, 1):
                    option_code = opt.get('option_code', '')
                    option_type = "Call" if "C" in option_code else "Put"
                    price = opt.get('price', 0)
                    volume = opt.get('volume', 0)
                    volume_diff = opt.get('volume_diff', 0)
                    amount = opt.get('turnover', 0)  # 使用turnover字段作为金额
                    
                    report_lines.append(
                        f"  {i}. {option_code}: {option_type}, "
                        f"{price:.3f}×{volume}张, +{volume_diff}张, "
                        f"{amount/10000:.1f}万"
                    )
            
            # 发送通知
            report_text = "\n".join(report_lines)
            self.notifier.send_wework_notification(report_text)
            self.logger.info(f"V2系统发送汇总报告: {total_trades}笔交易, {len(stock_summary)}只股票")
            
        except Exception as e:
            self.logger.error(f"V2系统发送汇总报告失败: {e}")
    
    def load_previous_options(self):
        """从数据库加载历史期权数据作为比较基准"""
        try:
            # 直接从数据库加载最近2小时的期权数据
            recent_data = self.data_handler.load_recent_option_data(hours=2)
            
            # 转换为字典结构
            self.previous_options = {}
            for opt in recent_data:
                option_code = opt.get('option_code', '')
                if option_code:
                    self.previous_options[option_code] = opt
            
            self.logger.info(f"V2系统从数据库加载历史期权数据: {len(self.previous_options)} 条记录")
            
        except Exception as e:
            self.logger.error(f"V2系统从数据库加载历史期权数据失败: {e}")
            self.previous_options = {}
    
    def compare_with_previous_options(self, current_options: List[Dict]) -> List[Dict]:
        """使用已计算好的变化量进行过滤，基于真实的历史数据比较"""
        try:
            # 🔥 新逻辑：直接使用 big_options_processor 中已经计算好的 volume_diff
            # 这个 volume_diff 是基于数据库中的历史数据计算的，更准确
            options_with_diff = []
            for current_opt in current_options:
                option_code = current_opt.get('option_code', '')
                
                # 使用已经计算好的变化量（在 big_options_processor 中基于数据库历史数据计算）
                current_volume = current_opt.get('volume', 0)
                volume_diff = current_opt.get('volume_diff', 0)
                previous_volume = current_opt.get('last_volume', 0)
                current_turnover = current_opt.get('turnover', 0)
                
                # 如果没有 volume_diff 字段，说明数据有问题，跳过
                if 'volume_diff' not in current_opt:
                    self.logger.warning(f"期权 {option_code} 缺少 volume_diff 字段，跳过")
                    continue
                
                # 直接使用传入的数据，不修改
                opt_with_diff = current_opt.copy()
                
                # 获取该期权的过滤配置
                stock_code = current_opt.get('stock_code', '')
                
                # 获取增量阈值配置
                from config import OPTION_FILTERS, get_market_type
                market_type = get_market_type(stock_code)
                default_key = f'{market_type.lower()}_default'
                
                # 优先使用股票特定配置，否则使用默认配置
                filter_config = OPTION_FILTERS.get(stock_code, OPTION_FILTERS.get(default_key, {}))
                min_volume_diff = filter_config.get('min_volume_diff', 10)  # 默认最小增量10张
                min_turnover = filter_config.get('min_turnover', 100000)  # 默认最小成交额
                
                # 🔥 修改通知条件：基于真实的数据变化
                # 1. 必须有正向的成交量变化 (volume_diff > 0)
                # 2. 变化量必须达到阈值 (volume_diff >= min_volume_diff)
                # 3. 成交额必须达到阈值 (current_turnover >= min_turnover)
                has_positive_change = volume_diff > 0
                meets_volume_threshold = volume_diff >= min_volume_diff
                meets_turnover_threshold = current_turnover >= min_turnover
                
                if has_positive_change and meets_volume_threshold and meets_turnover_threshold:
                    # 满足通知条件
                    options_with_diff.append(opt_with_diff)
                    self.logger.info(f"✅ 期权符合通知条件 {option_code}: "
                                   f"当前成交量={current_volume:,}, 上次={previous_volume:,}, "
                                   f"变化量={volume_diff:+,}, 成交额={current_turnover:,.0f} "
                                   f"(阈值: 变化量>={min_volume_diff}, 成交额>={min_turnover:,.0f})")
                else:
                    # 不满足通知条件，记录原因
                    reasons = []
                    if not has_positive_change:
                        reasons.append(f"无正向变化(diff={volume_diff})")
                    if not meets_volume_threshold:
                        reasons.append(f"变化量不足(diff={volume_diff}<{min_volume_diff})")
                    if not meets_turnover_threshold:
                        reasons.append(f"成交额不足({current_turnover:,.0f}<{min_turnover:,.0f})")
                    
                    self.logger.info(f"❌ 期权不符合通知条件 {option_code}: {', '.join(reasons)}")
              
            self.logger.info(f"V2系统期权通知过滤: {len(current_options)} -> {len(options_with_diff)} (符合通知条件)")
            return options_with_diff
            
        except Exception as e:
            self.logger.error(f"V2系统期权通知过滤失败: {e}")
            # 如果过滤失败，返回空列表，避免发送错误通知
            return []
    
    def _check_connection(self) -> bool:
        """检查富途连接状态"""
        try:
            if not self.quote_ctx:
                return False
            
            # 尝试获取市场快照来测试连接
            ret, data = self.quote_ctx.get_market_snapshot(['HK.00700'])
            return ret == ft.RET_OK
            
        except Exception as e:
            self.logger.warning(f"V2系统连接检查失败: {e}")
            return False
    
    def ensure_connection(self) -> bool:
        """确保富途连接可用"""
        if self._check_connection():
            return True
        
        self.logger.info("V2系统连接不可用，尝试重新连接...")
        return self.connect_futu()
    
    def manual_scan(self) -> List[Dict]:
        """手动扫描一次"""
        self.logger.info("V2系统执行手动扫描...")
        
        # 确保连接可用
        if not self.ensure_connection():
            self.logger.error("V2系统无法连接富途OpenD")
            return []
        
        # 手动扫描时也加载历史数据
        if not self.previous_options:
            self.load_previous_options()
        
        return self.scan_big_options()


def signal_handler(signum, frame):
    """信号处理器"""
    print(f"\nV2系统收到信号 {signum}，正在优雅退出...")
    if 'monitor' in globals():
        monitor.stop_monitoring()
    sys.exit(0)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='V2系统港股期权大单监控')
    parser.add_argument('--mode', choices=['monitor', 'scan', 'status', 'test'], 
                       default='monitor', help='运行模式')
    parser.add_argument('--config-check', action='store_true', help='检查配置')
    
    args = parser.parse_args()
    
    # 配置检查
    if args.config_check:
        errors = validate_config()
        if errors:
            print("V2系统配置验证失败:")
            for error in errors:
                print(f"  - {error}")
            return 1
        else:
            print("V2系统配置验证通过")
            return 0
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 创建监控器
    global monitor
    monitor = V2OptionMonitor()
    
    try:
        if args.mode == 'monitor':
            # 持续监控模式
            monitor.start_monitoring()
            
            # 保持主线程运行
            try:
                while monitor.is_running:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            
        elif args.mode == 'scan':
            # 单次扫描模式
            big_options = monitor.manual_scan()
            print(f"V2系统扫描完成，发现 {len(big_options)} 笔大单期权")
            
            if big_options:
                for i, option in enumerate(big_options[:5], 1):  # 显示前5个
                    print(f"{i}. {option.get('stock_name')} {option.get('option_code')} "
                          f"成交额: {option.get('turnover', 0):,.0f}港币")
            
        elif args.mode == 'status':
            # 状态查看模式
            status = monitor.get_status()
            print("V2系统监控状态:")
            for key, value in status.items():
                print(f"  {key}: {value}")
                
        elif args.mode == 'test':
            # 测试模式
            print("V2系统测试模式...")
            if monitor.connect_futu():
                print("✓ 富途连接正常")
                
                # 测试获取股价
                test_stock = 'HK.00700'
                price = monitor.get_stock_price(test_stock)
                print(f"✓ 获取股价正常: {test_stock} = {price}")
                
                # 测试通知
                monitor.notifier.send_wework_notification("V2系统测试通知")
                monitor.mac_notifier.send_notification("V2测试", "系统测试通知")
                print("✓ 通知功能测试完成")
                
                monitor.disconnect_futu()
            else:
                print("✗ 富途连接失败")
                return 1
    
    except Exception as e:
        print(f"V2系统运行异常: {e}")
        traceback.print_exc()
        return 1
    
    finally:
        if monitor.is_running:
            monitor.stop_monitoring()
    
    return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)