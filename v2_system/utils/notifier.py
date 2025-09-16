# -*- coding: utf-8 -*-
"""
V2系统通知模块 - 集成企微和Mac通知
"""

import logging
import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import sys
import os

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import NOTIFICATION, should_send_to_extra_webhooks
from .mac_notifier import MacNotifier
from .database_manager import V2DatabaseManager


class V2Notifier:
    """V2系统通知器 - 支持企微和Mac通知"""
    
    def __init__(self):
        self.logger = logging.getLogger('V2OptionMonitor.Notifier')
        self.mac_notifier = MacNotifier()
        self.notification_history = {}  # 通知历史记录
        self.last_summary_time = None
        self.db_manager = V2DatabaseManager()
        
    def send_wework_notification(self, message: str, mentioned_list: List[str] = None) -> bool:
        """发送企业微信通知"""
        if not NOTIFICATION.get('enable_wework_bot'):
            return False
            
        wework_config = NOTIFICATION.get('wework_config', {})
        webhook_url = wework_config.get('webhook_url')
        
        if not webhook_url:
            self.logger.warning("V2企业微信webhook URL未配置")
            return False
        
        try:
            # 构建消息体
            data = {
                "msgtype": "text",
                "text": {
                    "content": f"[V2系统] {message}",
                    "mentioned_list": mentioned_list or wework_config.get('mentioned_list', []),
                    "mentioned_mobile_list": wework_config.get('mentioned_mobile_list', [])
                }
            }
            
            # 发送主要webhook
            response = requests.post(
                webhook_url,
                json=data,
                timeout=10,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    self.logger.info("V2企业微信通知发送成功")
                    
                    # 只在开市时间向额外的webhook URL发送消息
                    if should_send_to_extra_webhooks():
                        extra_urls = wework_config.get('extra_webhook_urls', [])
                        for extra_url in extra_urls:
                            try:
                                requests.post(extra_url, json=data, timeout=5)
                                self.logger.info(f"V2额外webhook发送成功: {extra_url}")
                            except Exception as e:
                                self.logger.warning(f"V2额外webhook发送失败: {e}")
                    else:
                        self.logger.info("V2非开市时间，跳过额外webhook发送")
                    
                    return True
                else:
                    self.logger.error(f"V2企业微信通知发送失败: {result}")
                    return False
            else:
                self.logger.error(f"V2企业微信通知HTTP错误: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"V2企业微信通知发送异常: {e}")
            return False
    
    def send_mac_notification(self, title: str, message: str, subtitle: str = "") -> bool:
        """发送Mac系统通知"""
        if not NOTIFICATION.get('enable_mac_notification'):
            return False
            
        return self.mac_notifier.send_notification(title, message, subtitle)
    
    def send_big_option_alert(self, option_info: Dict[str, Any]) -> bool:
        """发送大单期权提醒"""
        try:
            # 构建通知消息
            stock_code = option_info.get('stock_code', '')
            stock_name = option_info.get('stock_name', '')
            option_code = option_info.get('option_code', '')
            volume = option_info.get('volume', 0)
            turnover = option_info.get('turnover', 0)
            price = option_info.get('price', 0)
            strike_price = option_info.get('strike_price', 0)
            option_type = option_info.get('option_type', '')
            direction = option_info.get('direction', 'Unknown')
            
            # 构建消息
            title = f"🔥 V2大单期权提醒"
            
            message_parts = [
                f"股票: {stock_name}({stock_code})",
                f"期权: {option_code}",
                f"执行价: {strike_price:.2f}",
                f"类型: {option_type}",
                f"成交量: {volume:,}张",
                f"成交额: {turnover:,.0f}港币",
                f"价格: {price:.4f}",
            ]
            
            message_parts.append(f"时间: {datetime.now().strftime('%H:%M:%S')}")
            
            message = "\n".join(message_parts)
            
            # 发送通知
            success = False
            
            # 发送企微通知
            if self.send_wework_notification(message):
                success = True
            
            # 发送Mac通知
            mac_message = f"{stock_name} 大单期权\n成交额: {turnover/10000:.1f}万港币"
            if self.send_mac_notification(title, mac_message):
                success = True
            
            # 控制台输出
            if NOTIFICATION.get('enable_console', True):
                print(f"\n{title}")
                print(message)
                success = True
            
            return success
            
        except Exception as e:
            self.logger.error(f"V2发送大单期权提醒失败: {e}")
            return False
    
    def send_summary_notification(self, big_options: List[Dict[str, Any]]) -> bool:
        """发送汇总通知"""
        if not big_options:
            return False
        
        try:
            current_time = datetime.now()
            
            # 检查汇总通知间隔
            if self.last_summary_time:
                interval = NOTIFICATION.get('wework_config', {}).get('summary_interval', 300)
                if (current_time - self.last_summary_time).seconds < interval:
                    return False
            
            total_count = len(big_options)
            total_turnover = sum(opt.get('turnover', 0) for opt in big_options)
            
            # 按股票分组统计
            stock_stats = {}
            for opt in big_options:
                stock_code = opt.get('stock_code', 'Unknown')
                stock_name = opt.get('stock_name', stock_code)
                if stock_code not in stock_stats:
                    stock_stats[stock_code] = {
                        'name': stock_name,
                        'count': 0, 
                        'turnover': 0
                    }
                stock_stats[stock_code]['count'] += 1
                stock_stats[stock_code]['turnover'] += opt.get('turnover', 0)
            
            # 构建汇总消息
            title = "📊 V2期权大单汇总"
            
            message_parts = [
                f"时间段汇总 ({current_time.strftime('%H:%M')})",
                f"总交易数: {total_count}笔",
                f"总金额: {total_turnover/10000:.1f}万港币",
                ""
            ]
            
            # 添加股票统计（按成交额排序）
            sorted_stocks = sorted(stock_stats.items(), 
                                 key=lambda x: x[1]['turnover'], 
                                 reverse=True)
            
            message_parts.append("分股票统计:")
            for stock_code, stats in sorted_stocks[:5]:  # 只显示前5个
                message_parts.append(
                    f"• {stats['name']}: {stats['count']}笔, "
                    f"{stats['turnover']/10000:.1f}万港币"
                )
            
            if len(sorted_stocks) > 5:
                message_parts.append(f"... 还有{len(sorted_stocks)-5}只股票")
            
            message = "\n".join(message_parts)
            
            # 发送通知
            success = False
            
            if self.send_wework_notification(message):
                success = True
            
            # Mac汇总通知
            self.mac_notifier.send_big_options_summary(big_options)
            
            if success:
                self.last_summary_time = current_time
            
            return success
            
        except Exception as e:
            self.logger.error(f"V2发送汇总通知失败: {e}")
            return False
    
    def send_stock_grouped_notifications(self, big_options: List[Dict[str, Any]]) -> bool:
        """发送按股票分组的期权通知，每个股票显示变动最大的前3个"""
        if not big_options:
            return False
        
        try:
            # 过滤出有变化的期权（volume_diff > 0 或者是当日开盘后首次记录）
            changed_options = []
            for opt in big_options:
                volume_diff = opt.get('volume_diff', 0)
                # 当日开盘后首次记录的期权也应该包含在内
                if volume_diff > 0 or (volume_diff == 0 and opt.get('last_volume', 0) == opt.get('volume', 0) and opt.get('volume', 0) > 0):
                    changed_options.append(opt)
            
            if not changed_options:
                self.logger.info("V2没有期权成交量变化或开盘后首次记录，跳过通知")
                return False
            
            # 按股票分组
            stock_groups = {}
            for option in changed_options:
                stock_code = option.get('stock_code', 'Unknown')
                stock_name = option.get('stock_name', stock_code)
                
                if stock_code not in stock_groups:
                    stock_groups[stock_code] = {
                        'name': stock_name,
                        'options': []
                    }
                stock_groups[stock_code]['options'].append(option)
            
            # 为每个股票发送通知
            success_count = 0
            for stock_code, group_data in stock_groups.items():
                stock_name = group_data['name']
                options = group_data['options']
                
                # 按成交额排序，取前3个
                top_options = sorted(options, key=lambda x: x.get('turnover', 0), reverse=True)[:3]
                
                if self._send_stock_group_notification(stock_code, stock_name, top_options):
                    success_count += 1
            
            self.logger.info(f"V2发送了 {success_count}/{len(stock_groups)} 个股票的分组通知 (共{len(changed_options)}个有变化的期权)")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"V2发送股票分组通知失败: {e}")
            return False
    
    def _send_stock_group_notification(self, stock_code: str, stock_name: str, options: List[Dict[str, Any]]) -> bool:
        """发送单个股票的期权通知"""
        if not options:
            return False
        
        try:
            # 检查通知间隔
            notification_key = f"stock_group_{stock_code}"
            if not self._should_send_stock_notification(notification_key):
                return False
            
            # 计算汇总数据
            total_turnover = sum(opt.get('turnover', 0) for opt in options)
            total_volume = sum(opt.get('volume', 0) for opt in options)
            
            # 构建消息
            title = f"🔥 V2大单期权 - {stock_name}"
            
            message_parts = [
                f"股票: {stock_name}({stock_code})",
                f"发现 {len(options)} 笔大单期权",
                f"总成交额: {total_turnover/10000:.1f}万港币",
                f"总成交量: {total_volume:,}张",
                "",
                "详情 (按成交额排序):"
            ]
            
            # 添加每个期权的详情
            for i, option in enumerate(options, 1):
                option_code = option.get('option_code', '')
                volume = option.get('volume', 0)
                turnover = option.get('turnover', 0)
                price = option.get('price', 0)
                strike_price = option.get('strike_price', 0)
                option_type = option.get('option_type', '')
                direction = option.get('direction', 'Unknown')
                
                # 获取变化量信息
                volume_diff = option.get('volume_diff', 0)
                last_volume = option.get('last_volume', 0)
                
                # 构建成交量显示（包含变化量）
                if volume_diff > 0:
                    volume_display = f"{volume:,}张 (+{volume_diff:,})"
                else:
                    volume_display = f"{volume:,}张"
                
                message_parts.append(
                    f"{i}. {option_type} {strike_price:.2f}"
                )
                message_parts.append(
                    f"   成交: {volume_display}, {turnover/10000:.1f}万港币, 价格: {price:.4f}"
                )
            
            message_parts.append(f"\n时间: {datetime.now().strftime('%H:%M:%S')}")
            
            message = "\n".join(message_parts)
            
            # 发送通知
            success = False
            
            # 发送企微通知
            if self.send_wework_notification(message):
                success = True
            
            # 发送Mac通知
            mac_message = f"{len(options)}笔大单\n总额: {total_turnover/10000:.1f}万港币"
            if self.send_mac_notification(title, mac_message):
                success = True
            
            # 控制台输出
            if NOTIFICATION.get('enable_console', True):
                print(f"\n{title}")
                print(message)
                success = True
            
            if success:
                self.notification_history[notification_key] = datetime.now()
            
            return success
            
        except Exception as e:
            self.logger.error(f"V2发送股票分组通知失败 {stock_code}: {e}")
            return False
    
    def _should_send_stock_notification(self, notification_key: str) -> bool:
        """检查股票分组通知是否应该发送"""
        current_time = datetime.now()
        interval = NOTIFICATION.get('notification_interval', 60)  # 默认60秒间隔
        
        if notification_key in self.notification_history:
            last_time = self.notification_history[notification_key]
            if (current_time - last_time).seconds < interval:
                return False
        
        return True
    
    def should_send_notification(self, option_code: str) -> bool:
        """检查是否应该发送通知（避免重复）"""
        current_time = datetime.now()
        interval = NOTIFICATION.get('notification_interval', 60)
        
        if option_code in self.notification_history:
            last_time = self.notification_history[option_code]
            if (current_time - last_time).seconds < interval:
                return False
        
        self.notification_history[option_code] = current_time
        return True
    
    def send_v1_style_summary_report(self, big_options: List[Dict[str, Any]]) -> bool:
        """发送V1风格的期权监控汇总报告"""
        if not big_options:
            return False
        
        try:
            # 过滤出有变化的期权（volume_diff > 0 或者是当日开盘后首次记录）
            changed_options = []
            for opt in big_options:
                volume_diff = opt.get('volume_diff', 0)
                # 当日开盘后首次记录的期权也应该包含在内
                if volume_diff > 0 or (volume_diff == 0 and opt.get('last_volume', 0) == opt.get('volume', 0) and opt.get('volume', 0) > 0):
                    changed_options.append(opt)
            
            if not changed_options:
                self.logger.info("V2没有期权成交量变化或开盘后首次记录，跳过汇总报告")
                return False
            
            current_time = datetime.now()
            
            # 计算总体统计
            total_trades = len(big_options)
            new_trades = len(changed_options)
            qualified_trades = len([opt for opt in changed_options if opt.get('turnover', 0) >= 1000000])  # 100万港币以上
            
            total_amount = sum(opt.get('turnover', 0) for opt in big_options)
            new_amount = sum(opt.get('turnover', 0) for opt in changed_options)
            qualified_amount = sum(opt.get('turnover', 0) for opt in changed_options if opt.get('turnover', 0) >= 1000000)
            
            # 按股票分组统计
            stock_groups = {}
            for option in changed_options:
                stock_code = option.get('stock_code', 'Unknown')
                stock_name = option.get('stock_name', stock_code)
                
                # 尝试从数据库获取股票信息
                stock_info = self.db_manager.get_stock_info(stock_code)
                if stock_info:
                    stock_name = stock_info.get('stock_name', stock_name)
                    current_price = stock_info.get('current_price')
                else:
                    current_price = None
                
                if stock_code not in stock_groups:
                    stock_groups[stock_code] = {
                        'name': stock_name,
                        'current_price': current_price,
                        'count': 0,
                        'turnover': 0,
                        'options': []
                    }
                
                stock_groups[stock_code]['count'] += 1
                stock_groups[stock_code]['turnover'] += option.get('turnover', 0)
                stock_groups[stock_code]['options'].append(option)
            
            # 构建汇总报告
            message_parts = [
                "📊 期权监控汇总报告",
                f"⏰ 时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"📈 总交易: {total_trades} 笔 (新增: {new_trades} 笔，符合通知条件: {qualified_trades} 笔)",
                f"💰 总金额: {total_amount:,.0f} 港币 (新增: {new_amount:,.0f} 港币，符合条件: {qualified_amount:,.0f} 港币)",
                "",
                "📋 新增大单统计:"
            ]
            
            # 按成交额排序股票
            sorted_stocks = sorted(stock_groups.items(), 
                                 key=lambda x: x[1]['turnover'], 
                                 reverse=True)
            
            for stock_code, group_data in sorted_stocks:
                stock_name = group_data['name']
                current_price = group_data['current_price']
                count = group_data['count']
                turnover = group_data['turnover']
                options = group_data['options']
                
                # 股票标题行
                if current_price:
                    stock_title = f"• {stock_name} ({stock_code}): {count}笔, {turnover:,.0f}港币 (股价: {current_price:.2f})"
                else:
                    stock_title = f"• {stock_name} ({stock_code}): {count}笔, {turnover:,.0f}港币"
                
                message_parts.append(stock_title)
                
                # 按成交额排序期权，取前3个
                top_options = sorted(options, key=lambda x: x.get('turnover', 0), reverse=True)[:3]
                
                for i, option in enumerate(top_options, 1):
                    option_code = option.get('option_code', '')
                    option_type = option.get('option_type', '')
                    strike_price = option.get('strike_price', 0)
                    price = option.get('price', 0)
                    volume = option.get('volume', 0)
                    volume_diff = option.get('volume_diff', 0)
                    turnover = option.get('turnover', 0)
                    
                    # 获取未平仓合约数变化信息
                    open_interest_diff = option.get('open_interest_diff', 0)
                    net_open_interest_diff = option.get('net_open_interest_diff', 0)
                    
                    # 构建期权详情行（包含未平仓合约数变化）
                    option_detail = (
                        f"  {i}. {option_code}: {option_type}, "
                        f"{price:.3f}×{volume:,}张, +{volume_diff:,}张, "
                        f"{turnover/10000:.1f}万"
                    )
                    
                    # 添加未平仓合约数变化信息（如果有变化）
                    if open_interest_diff != 0 or net_open_interest_diff != 0:
                        oi_parts = []
                        if open_interest_diff != 0:
                            oi_parts.append(f"持仓{open_interest_diff:+,}")
                        if net_open_interest_diff != 0:
                            oi_parts.append(f"净持仓{net_open_interest_diff:+,}")
                        if oi_parts:
                            option_detail += f", {', '.join(oi_parts)}"
                    message_parts.append(option_detail)
            
            message = "\n".join(message_parts)
            
            # 发送通知
            success = False
            
            # 发送企微通知
            if self.send_wework_notification(message):
                success = True
            
            # 发送Mac通知
            mac_title = "📊 V2期权监控汇总报告"
            mac_message = f"{new_trades}笔新增交易\n总额: {new_amount/10000:.1f}万港币"
            if self.send_mac_notification(mac_title, mac_message):
                success = True
            
            # 控制台输出
            if NOTIFICATION.get('enable_console', True):
                print(f"\n{message}")
                success = True
            
            if success:
                self.last_summary_time = current_time
            
            return success
            
        except Exception as e:
            self.logger.error(f"V2发送V1风格汇总报告失败: {e}")
            return False
    
    def update_stock_info_cache(self, stock_code: str, stock_name: str = None, current_price: float = None) -> bool:
        """更新股票信息缓存到数据库"""
        try:
            return self.db_manager.save_stock_info(stock_code, stock_name, current_price)
        except Exception as e:
            self.logger.error(f"V2更新股票信息缓存失败 {stock_code}: {e}")
            return False