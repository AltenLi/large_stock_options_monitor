# SPDX-License-Identifier: GPL-3.0-or-later
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
                    "content": f"{message}",
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
    
    def send_individual_stock_notification(self, stock_code: str, stock_name: str, 
                                         current_price: float, options: List[Dict[str, Any]]) -> bool:
        """发送单个股票的期权监控通知"""
        if not options:
            return False
        
        try:
            current_time = datetime.now()
            
            # 按新增张数降序排序
            sorted_options = sorted(options, key=lambda x: x.get('volume_diff', 0), reverse=True)
            
            # 构建消息
            message_parts = [
                "📊 期权大单监控",
                f"⏰ 时间: {current_time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"• {stock_name} ({stock_code}): （股价: {current_price:.2f}）"
            ]
            
            # 添加所有满足条件的期权
            for i, option in enumerate(sorted_options, 1):
                option_code = option.get('option_code', '')
                option_type = option.get('option_type', '')
                price = option.get('price', 0)
                volume = option.get('volume', 0)
                volume_diff = option.get('volume_diff', 0)
                turnover = option.get('turnover', 0)
                
                # 获取持仓相关信息
                option_open_interest = option.get('option_open_interest', 0)
                option_net_open_interest = option.get('option_net_open_interest', 0)
                open_interest_diff = option.get('open_interest_diff', 0)
                net_open_interest_diff = option.get('net_open_interest_diff', 0)
                
                # 构建期权详情行
                option_detail = (
                    f"{i}. {option_code}: 【{option_type}】, "
                    f"{price:.3f}×{volume:,}张, +{volume_diff:,}张, "
                    f"{turnover/10000:.1f}万, "
                    f"持仓: {option_open_interest:,}张（{open_interest_diff:+,}）, "
                    f"净持仓: {option_net_open_interest:,}张（{net_open_interest_diff:+,}）"
                )
                
                message_parts.append(option_detail)
            
            message = "\n".join(message_parts)
            
            # 发送通知
            success = False
            
            # 发送企微通知
            if self.send_wework_notification(message):
                success = True
            
            # 发送Mac通知
            mac_title = f"📊 {stock_name} 期权大单"
            mac_message = f"{len(options)}个期权满足条件\n股价: {current_price:.2f}"
            if self.send_mac_notification(mac_title, mac_message):
                success = True
            
            # 控制台输出
            if NOTIFICATION.get('enable_console', True):
                print(f"\n{message}")
                success = True
            
            return success
            
        except Exception as e:
            self.logger.error(f"V2发送单个股票通知失败 {stock_code}: {e}")
            return False

    def send_v1_style_summary_report(self, big_options: List[Dict[str, Any]]) -> bool:
        """发送V1风格的期权监控汇总报告（保留作为备用）"""
        if not big_options:
            return False
        
        try:
            # 🔥 修改：直接使用传入的期权数据，因为已经在 compare_with_previous_options 中过滤过了
            # 传入的 big_options 已经是符合通知条件的期权
            changed_options = big_options
            
            if not changed_options:
                self.logger.info("V2没有符合通知条件的期权，跳过汇总报告")
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
                    
                    # 获取持仓相关信息
                    option_open_interest = option.get('option_open_interest', 0)
                    option_net_open_interest = option.get('option_net_open_interest', 0)
                    open_interest_diff = option.get('open_interest_diff', 0)
                    net_open_interest_diff = option.get('net_open_interest_diff', 0)
                    
                    # 构建期权详情行（包含未平仓合约数变化）
                    option_detail = (
                        f"  {i}. {option_code}: {option_type}, "
                        f"{price:.3f}×{volume:,}张, +{volume_diff:,}张, "
                        f"{turnover/10000:.1f}万, "
                        f"持仓: {option_open_interest:,}张"
                        f"（{open_interest_diff:+,}）, "
                        f"净持仓: {option_net_open_interest:,}张"
                        f"（{net_open_interest_diff:+,}）"
                    )
                    
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
