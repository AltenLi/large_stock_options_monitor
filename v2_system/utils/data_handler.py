# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
V2系统数据处理模块
"""

import json
import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SYSTEM_CONFIG, get_database_config
from .database_manager import get_database_manager


class V2DataHandler:
    """V2系统数据处理器"""
    
    def __init__(self, market: str = 'HK'):
        self.market = market
        self.logger = logging.getLogger(f'V2OptionMonitor.DataHandler.{market}')
        self.cache_dir = SYSTEM_CONFIG['cache_dir']
        self.stock_info_file = SYSTEM_CONFIG['stock_info_cache']
        self.price_cache_file = SYSTEM_CONFIG['price_cache']
        self.db_manager = get_database_manager(market)
        
        # 确保缓存目录存在（用于临时文件和导出）
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.stock_info_file), exist_ok=True)
    
    def save_stock_prices(self, stock_prices: Dict[str, Any]) -> bool:
        """保存股票价格到缓存文件"""
        try:
            cache_data = {
                'update_time': datetime.now().isoformat(),
                'prices': stock_prices
            }
            
            with open(self.price_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"V2股票价格缓存已保存: {len(stock_prices)}只股票")
            return True
            
        except Exception as e:
            self.logger.error(f"V2保存股票价格缓存失败: {e}")
            return False
    
    def load_stock_prices(self) -> Dict[str, Any]:
        """从缓存文件加载股票价格"""
        try:
            if not os.path.exists(self.price_cache_file):
                return {}
            
            with open(self.price_cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            # 检查缓存是否过期（5分钟）
            update_time_str = cache_data.get('update_time')
            if update_time_str:
                update_time = datetime.fromisoformat(update_time_str)
                if (datetime.now() - update_time).seconds > 300:
                    self.logger.info("V2股票价格缓存已过期")
                    return {}
            
            prices = cache_data.get('prices', {})
            self.logger.info(f"V2从缓存加载股票价格: {len(prices)}只股票")
            return prices
            
        except Exception as e:
            self.logger.error(f"V2加载股票价格缓存失败: {e}")
            return {}
    
    def save_stock_info(self, stock_info: Dict[str, Any]) -> bool:
        """保存股票基本信息"""
        try:
            info_data = {
                'update_time': datetime.now().isoformat(),
                'stocks': stock_info
            }
            
            with open(self.stock_info_file, 'w', encoding='utf-8') as f:
                json.dump(info_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"V2股票基本信息已保存: {len(stock_info)}只股票")
            return True
            
        except Exception as e:
            self.logger.error(f"V2保存股票基本信息失败: {e}")
            return False
    
    def load_stock_info(self) -> Dict[str, Any]:
        """加载股票基本信息"""
        try:
            if not os.path.exists(self.stock_info_file):
                return {}
            
            with open(self.stock_info_file, 'r', encoding='utf-8') as f:
                info_data = json.load(f)
            
            stocks = info_data.get('stocks', {})
            self.logger.info(f"V2从缓存加载股票基本信息: {len(stocks)}只股票")
            return stocks
            
        except Exception as e:
            self.logger.error(f"V2加载股票基本信息失败: {e}")
            return {}
    
    def save_option_data(self, option_data: List[Dict[str, Any]]) -> bool:
        """保存期权数据到数据库"""
        try:
            if not option_data:
                return True
            
            # 过滤成交量为0的期权，减少磁盘消耗
            filtered_options = [option for option in option_data if option.get('volume', 0) > 0]
            
            if len(filtered_options) != len(option_data):
                skipped_count = len(option_data) - len(filtered_options)
                self.logger.info(f"V2跳过{skipped_count}个成交量为0的期权，减少磁盘消耗")
            
            if not filtered_options:
                self.logger.info("V2所有期权成交量都为0，跳过数据库保存")
                return True
            
            success_count = 0
            for option in filtered_options:
                if self.db_manager.save_option_trade(option):
                    success_count += 1
            
            self.logger.info(f"V2期权数据已保存到数据库: {success_count}/{len(filtered_options)}条记录")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"V2保存期权数据到数据库失败: {e}")
            return False
    
    def load_recent_option_data(self, days: int = 7, hours: int = None) -> List[Dict[str, Any]]:
        """从数据库加载最近的期权数据"""
        try:
            if hours:
                # 按小时加载
                all_data = self.db_manager.get_recent_option_trades(hours=hours)
            else:
                # 按天加载（转换为小时）
                all_data = self.db_manager.get_recent_option_trades(hours=days*24)
            
            self.logger.info(f"V2从数据库加载最近期权数据: {len(all_data)}条记录")
            return all_data
            
        except Exception as e:
            self.logger.error(f"V2从数据库加载最近期权数据失败: {e}")
            return []
    
    def cleanup_old_data(self, keep_days: int = 30) -> bool:
        """清理旧数据（数据库和文件）"""
        try:
            # 清理数据库中的旧数据
            db_cleaned = self.db_manager.cleanup_old_data(keep_days)
            
            # 清理旧的JSON文件（如果还有的话）
            file_cleaned_count = 0
            if os.path.exists(self.cache_dir):
                cutoff_date = datetime.now() - timedelta(days=keep_days)
                
                for filename in os.listdir(self.cache_dir):
                    if filename.startswith('options_') and filename.endswith('.json'):
                        try:
                            date_str = filename[8:18]  # 提取日期部分
                            file_date = datetime.strptime(date_str, '%Y-%m-%d')
                            
                            if file_date < cutoff_date:
                                file_path = os.path.join(self.cache_dir, filename)
                                os.remove(file_path)
                                file_cleaned_count += 1
                                self.logger.info(f"V2删除旧JSON文件: {filename}")
                        except Exception as e:
                            self.logger.warning(f"V2处理文件{filename}失败: {e}")
            
            self.logger.info(f"V2数据清理完成，数据库清理: {db_cleaned}, 文件清理: {file_cleaned_count}个")
            return True
            
        except Exception as e:
            self.logger.error(f"V2数据清理失败: {e}")
            return False
    
    def export_to_csv(self, data: List[Dict[str, Any]], filename: str) -> bool:
        """导出数据到CSV文件"""
        try:
            if not data:
                return True
            
            df = pd.DataFrame(data)
            csv_file = os.path.join(self.cache_dir, filename)
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            self.logger.info(f"V2数据已导出到CSV: {csv_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"V2导出CSV失败: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取数据统计信息"""
        try:
            stats = {
                'cache_dir': self.cache_dir,
                'cache_files': [],
                'total_size': 0,
                'file_count': 0
            }
            
            if os.path.exists(self.cache_dir):
                for filename in os.listdir(self.cache_dir):
                    file_path = os.path.join(self.cache_dir, filename)
                    if os.path.isfile(file_path):
                        file_size = os.path.getsize(file_path)
                        stats['cache_files'].append({
                            'name': filename,
                            'size': file_size,
                            'modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                        })
                        stats['total_size'] += file_size
                        stats['file_count'] += 1
            
            return stats
            
        except Exception as e:
            self.logger.error(f"V2获取统计信息失败: {e}")
            return {}
