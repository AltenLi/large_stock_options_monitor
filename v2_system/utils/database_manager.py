# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
V2系统数据库管理器
"""

import sqlite3
import os
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import sys

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_database_config


class V2DatabaseManager:
    """V2系统数据库管理器"""
    
    def __init__(self, market: str = 'HK'):
        self.market = market
        self.logger = logging.getLogger(f'V2OptionMonitor.DatabaseManager.{market}')
        
        # 根据市场获取数据库配置
        db_config = get_database_config(market)
        self.db_path = db_config['db_path']
        self.batch_size = db_config.get('batch_size', 1000)
        
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # 初始化数据库
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 创建股票基本信息表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stock_info (
                        stock_code TEXT PRIMARY KEY,
                        stock_name TEXT,
                        current_price REAL,
                        market_cap REAL,
                        lot_size INTEGER,
                        currency TEXT DEFAULT 'HKD',
                        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # 创建期权交易记录表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS option_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT,
                        option_code TEXT NOT NULL,
                        trade_date DATE NOT NULL,
                        timestamp DATETIME NOT NULL,
                        price REAL,
                        volume INTEGER,
                        turnover REAL,
                        change_rate REAL,
                        strike_price REAL,
                        option_type TEXT,
                        expiry_date DATE,
                        stock_price REAL,
                        price_diff REAL,
                        price_diff_pct REAL,
                        volume_diff INTEGER,
                        last_volume INTEGER,
                        option_open_interest INTEGER DEFAULT 0,
                        option_net_open_interest INTEGER DEFAULT 0,
                        open_interest_diff INTEGER DEFAULT 0,
                        net_open_interest_diff INTEGER DEFAULT 0,
                        data_type TEXT DEFAULT 'v2_current',
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(option_code, timestamp)
                    )
                ''')
                
                # 创建索引
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_option_trades_code_date 
                    ON option_trades(option_code, trade_date)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_option_trades_stock_date 
                    ON option_trades(stock_code, trade_date)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_option_trades_timestamp 
                    ON option_trades(timestamp)
                ''')
                
                # 创建股票信息表索引
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_stock_info_updated 
                    ON stock_info(last_updated)
                ''')
                
                conn.commit()
                self.logger.info(f"V2数据库初始化完成 ({self.market}市场)")
                
        except Exception as e:
            self.logger.error(f"V2数据库初始化失败: {e}")
            raise
    
    def save_option_trade(self, trade_data: Dict[str, Any]) -> bool:
        """保存单个期权交易记录"""
        try:
            # 过滤成交量为0的期权，减少磁盘消耗
            volume = trade_data.get('volume', 0)
            if volume <= 0:
                self.logger.debug(f"V2跳过保存成交量为0的期权: {trade_data.get('option_code')}")
                return True  # 返回True表示处理成功，只是跳过了保存
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 提取交易日期
                timestamp = trade_data.get('timestamp', datetime.now().isoformat())
                if isinstance(timestamp, str):
                    trade_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).date()
                else:
                    trade_date = timestamp.date()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO option_trades (
                        stock_code, stock_name, option_code, trade_date, timestamp,
                        price, volume, turnover, change_rate, strike_price,
                        option_type, expiry_date, stock_price, price_diff,
                        price_diff_pct, volume_diff, last_volume, 
                        option_open_interest, option_net_open_interest,
                        open_interest_diff, net_open_interest_diff, data_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data.get('stock_code'),
                    trade_data.get('stock_name'),
                    trade_data.get('option_code'),
                    trade_date,
                    timestamp,
                    trade_data.get('price'),
                    trade_data.get('volume'),
                    trade_data.get('turnover'),
                    trade_data.get('change_rate'),
                    trade_data.get('strike_price'),
                    trade_data.get('option_type'),
                    trade_data.get('expiry_date'),
                    trade_data.get('stock_price'),
                    trade_data.get('price_diff'),
                    trade_data.get('price_diff_pct'),
                    trade_data.get('volume_diff'),
                    trade_data.get('last_volume'),
                    trade_data.get('option_open_interest', 0),
                    trade_data.get('option_net_open_interest', 0),
                    trade_data.get('open_interest_diff', 0),
                    trade_data.get('net_open_interest_diff', 0),
                    trade_data.get('data_type', 'v2_current')
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"V2保存期权交易记录失败: {e}")
            return False
    
    def save_option_trades_batch(self, trades_data: List[Dict[str, Any]]) -> bool:
        """批量保存期权交易记录"""
        try:
            # 过滤成交量为0的期权，减少磁盘消耗
            filtered_trades = [trade for trade in trades_data if trade.get('volume', 0) > 0]
            
            if len(filtered_trades) != len(trades_data):
                skipped_count = len(trades_data) - len(filtered_trades)
                self.logger.info(f"V2批量保存时跳过{skipped_count}个成交量为0的期权")
            
            if not filtered_trades:
                self.logger.debug("V2批量保存：所有期权成交量都为0，跳过保存")
                return True
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                records = []
                for trade_data in filtered_trades:
                    # 提取交易日期
                    timestamp = trade_data.get('timestamp', datetime.now().isoformat())
                    if isinstance(timestamp, str):
                        trade_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).date()
                    else:
                        trade_date = timestamp.date()
                    
                    records.append((
                        trade_data.get('stock_code'),
                        trade_data.get('stock_name'),
                        trade_data.get('option_code'),
                        trade_date,
                        timestamp,
                        trade_data.get('price'),
                        trade_data.get('volume'),
                        trade_data.get('turnover'),
                        trade_data.get('change_rate'),
                        trade_data.get('strike_price'),
                        trade_data.get('option_type'),
                        trade_data.get('expiry_date'),
                        trade_data.get('stock_price'),
                        trade_data.get('price_diff'),
                        trade_data.get('price_diff_pct'),
                        trade_data.get('volume_diff'),
                        trade_data.get('last_volume'),
                        trade_data.get('option_open_interest', 0),
                        trade_data.get('option_net_open_interest', 0),
                        trade_data.get('open_interest_diff', 0),
                        trade_data.get('net_open_interest_diff', 0),
                        trade_data.get('data_type', 'v2_current')
                    ))
                
                cursor.executemany('''
                    INSERT OR REPLACE INTO option_trades (
                        stock_code, stock_name, option_code, trade_date, timestamp,
                        price, volume, turnover, change_rate, strike_price,
                        option_type, expiry_date, stock_price, price_diff,
                        price_diff_pct, volume_diff, last_volume, 
                        option_open_interest, option_net_open_interest,
                        open_interest_diff, net_open_interest_diff, data_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', records)
                
                conn.commit()
                self.logger.info(f"V2批量保存期权交易记录: {len(records)}条")
                return True
                
        except Exception as e:
            self.logger.error(f"V2批量保存期权交易记录失败: {e}")
            return False
    
    def get_today_option_volume(self, option_code: str, trade_date: Optional[str] = None) -> int:
        """获取指定期权当日最新成交量"""
        try:
            if trade_date is None:
                trade_date = datetime.now().date()
            elif isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT volume FROM option_trades 
                    WHERE option_code = ? AND trade_date = ?
                    ORDER BY timestamp DESC 
                    LIMIT 1
                ''', (option_code, trade_date))
                
                result = cursor.fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            self.logger.debug(f"V2获取期权{option_code}当日成交量失败: {e}")
            return 0

    def get_previous_option_volume(self, option_code: str, current_volume: int, trade_date: Optional[str] = None) -> int:
        """获取指定期权的上一条记录成交量（用于计算正确的变化量）"""
        try:
            if trade_date is None:
                trade_date = datetime.now().date()
            elif isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 🔥 修复：获取该期权当日最新的一条记录成交量
                # 如果当前成交量与最新记录相同，说明没有变化，返回当前成交量
                # 如果不同，返回最新记录的成交量用于计算diff
                cursor.execute('''
                    SELECT volume FROM option_trades 
                    WHERE option_code = ? AND trade_date = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                ''', (option_code, trade_date))
                
                result = cursor.fetchone()
                if result:
                    last_volume = result[0]
                    # 如果当前成交量与最新记录相同，说明没有新的交易
                    if current_volume == last_volume:
                        self.logger.debug(f"V2期权{option_code}成交量无变化: {current_volume}")
                        return current_volume  # 返回相同值，diff为0
                    else:
                        self.logger.debug(f"V2期权{option_code}成交量变化: {last_volume} -> {current_volume}")
                        return last_volume
                else:
                    # 没有历史记录，这是第一次记录
                    self.logger.debug(f"V2期权{option_code}首次记录成交量: {current_volume}")
                    return 0
                
        except Exception as e:
            self.logger.debug(f"V2获取期权{option_code}上一条记录成交量失败: {e}")
            return 0
    
    def get_previous_option_open_interest(self, option_code: str, current_open_interest: int, trade_date: Optional[str] = None) -> tuple:
        """获取指定期权的历史未平仓合约数，返回(previous_open_interest, previous_net_open_interest)"""
        try:
            if trade_date is None:
                trade_date = datetime.now().date()
            elif isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT option_open_interest, option_net_open_interest FROM option_trades 
                    WHERE option_code = ? AND trade_date = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                ''', (option_code, trade_date))
                
                result = cursor.fetchone()
                if result:
                    last_open_interest = result[0] or 0
                    last_net_open_interest = result[1] or 0
                    
                    # 如果当前未平仓合约数与最新记录相同，说明没有变化
                    if current_open_interest == last_open_interest:
                        self.logger.debug(f"V2期权{option_code}未平仓合约数无变化: {current_open_interest}")
                        return current_open_interest, last_net_open_interest  # 返回相同值，diff为0
                    else:
                        self.logger.debug(f"V2期权{option_code}未平仓合约数变化: {last_open_interest} -> {current_open_interest}")
                        return last_open_interest, last_net_open_interest
                else:
                    # 没有历史记录，这是第一次记录
                    self.logger.debug(f"V2期权{option_code}首次记录未平仓合约数: {current_open_interest}")
                    return 0, 0
                
        except Exception as e:
            self.logger.debug(f"V2获取期权{option_code}历史未平仓合约数失败: {e}")
            return 0, 0
    
    def get_today_all_option_volumes(self, trade_date: Optional[str] = None) -> Dict[str, int]:
        """获取当日所有期权的最新成交量"""
        try:
            if trade_date is None:
                trade_date = datetime.now().date()
            elif isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 获取每个期权当日最新的成交量记录
                cursor.execute('''
                    SELECT option_code, volume 
                    FROM option_trades t1
                    WHERE trade_date = ? 
                    AND timestamp = (
                        SELECT MAX(timestamp) 
                        FROM option_trades t2 
                        WHERE t2.option_code = t1.option_code 
                        AND t2.trade_date = ?
                    )
                ''', (trade_date, trade_date))
                
                results = cursor.fetchall()
                return {option_code: volume for option_code, volume in results}
                
        except Exception as e:
            self.logger.error(f"V2获取当日所有期权成交量失败: {e}")
            return {}

    def get_all_previous_option_volumes(self, current_volumes: Dict[str, int], trade_date: Optional[str] = None) -> Dict[str, int]:
        """批量获取所有期权的上一条记录成交量"""
        try:
            if trade_date is None:
                trade_date = datetime.now().date()
            elif isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            
            result = {}
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for option_code, current_volume in current_volumes.items():
                    # 🔥 修复：获取该期权当日最新的一条记录成交量
                    cursor.execute('''
                        SELECT volume FROM option_trades 
                        WHERE option_code = ? AND trade_date = ?
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ''', (option_code, trade_date))
                    
                    row = cursor.fetchone()
                    if row:
                        last_volume = row[0]
                        # 如果当前成交量与最新记录相同，说明没有新的交易
                        if current_volume == last_volume:
                            result[option_code] = current_volume  # 返回相同值，diff为0
                        else:
                            result[option_code] = last_volume
                    else:
                        # 没有历史记录，这是第一次记录
                        result[option_code] = 0
                
                return result
                
        except Exception as e:
            self.logger.error(f"V2批量获取期权上一条记录成交量失败: {e}")
            return {}
    
    def get_option_trades_by_date(self, trade_date: Optional[str] = None, 
                                  stock_code: Optional[str] = None) -> List[Dict[str, Any]]:
        """获取指定日期的期权交易记录"""
        try:
            if trade_date is None:
                trade_date = datetime.now().date()
            elif isinstance(trade_date, str):
                trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
            
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row  # 返回字典格式
                cursor = conn.cursor()
                
                if stock_code:
                    cursor.execute('''
                        SELECT * FROM option_trades 
                        WHERE trade_date = ? AND stock_code = ?
                        ORDER BY timestamp DESC
                    ''', (trade_date, stock_code))
                else:
                    cursor.execute('''
                        SELECT * FROM option_trades 
                        WHERE trade_date = ?
                        ORDER BY timestamp DESC
                    ''', (trade_date,))
                
                results = cursor.fetchall()
                return [dict(row) for row in results]
                
        except Exception as e:
            self.logger.error(f"V2获取期权交易记录失败: {e}")
            return []
    
    def cleanup_old_data(self, keep_days: int = 90):
        """清理旧数据"""
        try:
            cutoff_date = datetime.now().date() - timedelta(days=keep_days)
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    DELETE FROM option_trades 
                    WHERE trade_date < ?
                ''', (cutoff_date,))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                if deleted_count > 0:
                    self.logger.info(f"V2清理旧数据: 删除{deleted_count}条记录 (保留{keep_days}天)")
                
                return True
                
        except Exception as e:
            self.logger.error(f"V2清理旧数据失败: {e}")
            return False
    
    def save_stock_info(self, stock_code: str, stock_name: str = None, 
                       current_price: float = None, **kwargs) -> bool:
        """保存或更新股票基本信息"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 先查询现有记录
                cursor.execute('SELECT * FROM stock_info WHERE stock_code = ?', (stock_code,))
                existing = cursor.fetchone()
                
                if existing:
                    # 更新现有记录，只更新非空字段
                    update_fields = []
                    update_values = []
                    
                    if stock_name is not None:
                        update_fields.append('stock_name = ?')
                        update_values.append(stock_name)
                    
                    if current_price is not None:
                        update_fields.append('current_price = ?')
                        update_values.append(current_price)
                    
                    # 添加其他字段
                    for key, value in kwargs.items():
                        if value is not None and key in ['market_cap', 'lot_size', 'currency']:
                            update_fields.append(f'{key} = ?')
                            update_values.append(value)
                    
                    if update_fields:
                        update_fields.append('last_updated = ?')
                        update_values.append(datetime.now().isoformat())
                        update_values.append(stock_code)
                        
                        sql = f"UPDATE stock_info SET {', '.join(update_fields)} WHERE stock_code = ?"
                        cursor.execute(sql, update_values)
                else:
                    # 插入新记录
                    cursor.execute('''
                        INSERT INTO stock_info (
                            stock_code, stock_name, current_price, market_cap, 
                            lot_size, currency, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stock_code,
                        stock_name,
                        current_price,
                        kwargs.get('market_cap'),
                        kwargs.get('lot_size'),
                        kwargs.get('currency', 'HKD'),
                        datetime.now().isoformat()
                    ))
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"V2保存股票信息失败 {stock_code}: {e}")
            return False
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票基本信息"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM stock_info WHERE stock_code = ?', (stock_code,))
                result = cursor.fetchone()
                
                return dict(result) if result else None
                
        except Exception as e:
            self.logger.debug(f"V2获取股票信息失败 {stock_code}: {e}")
            return None
    
    def get_all_stock_info(self) -> Dict[str, Dict[str, Any]]:
        """获取所有股票基本信息"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('SELECT * FROM stock_info ORDER BY stock_code')
                results = cursor.fetchall()
                
                return {row['stock_code']: dict(row) for row in results}
                
        except Exception as e:
            self.logger.error(f"V2获取所有股票信息失败: {e}")
            return {}
    
    def batch_save_stock_info(self, stock_info_list: List[Dict[str, Any]]) -> bool:
        """批量保存股票信息"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                for stock_info in stock_info_list:
                    stock_code = stock_info.get('stock_code')
                    if not stock_code:
                        continue
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO stock_info (
                            stock_code, stock_name, current_price, market_cap,
                            lot_size, currency, last_updated
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stock_code,
                        stock_info.get('stock_name'),
                        stock_info.get('current_price'),
                        stock_info.get('market_cap'),
                        stock_info.get('lot_size'),
                        stock_info.get('currency', 'HKD'),
                        datetime.now().isoformat()
                    ))
                
                conn.commit()
                self.logger.info(f"V2批量保存股票信息: {len(stock_info_list)}只股票")
                return True
                
        except Exception as e:
            self.logger.error(f"V2批量保存股票信息失败: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # 总记录数
                cursor.execute('SELECT COUNT(*) FROM option_trades')
                total_records = cursor.fetchone()[0]
                
                # 今日记录数
                today = datetime.now().date()
                cursor.execute('SELECT COUNT(*) FROM option_trades WHERE trade_date = ?', (today,))
                today_records = cursor.fetchone()[0]
                
                # 股票信息记录数
                cursor.execute('SELECT COUNT(*) FROM stock_info')
                stock_records = cursor.fetchone()[0]
                
                # 数据日期范围
                cursor.execute('SELECT MIN(trade_date), MAX(trade_date) FROM option_trades')
                date_range = cursor.fetchone()
                
                # 数据库文件大小
                db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
                
                return {
                    'total_records': total_records,
                    'today_records': today_records,
                    'stock_records': stock_records,
                    'date_range': {
                        'start': date_range[0] if date_range[0] else None,
                        'end': date_range[1] if date_range[1] else None
                    },
                    'db_size_mb': round(db_size / 1024 / 1024, 2),
                    'db_path': self.db_path
                }
                
        except Exception as e:
            self.logger.error(f"V2获取数据库统计信息失败: {e}")
            return {}


    def get_recent_option_trades(self, hours: int = 2) -> List[Dict[str, Any]]:
        """获取最近几小时的期权交易记录"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # 计算时间范围
                cutoff_time = datetime.now() - timedelta(hours=hours)
                
                cursor.execute('''
                    SELECT * FROM option_trades 
                    WHERE timestamp >= ?
                    ORDER BY timestamp DESC
                ''', (cutoff_time.isoformat(),))
                
                results = cursor.fetchall()
                trades = [dict(row) for row in results]
                
                self.logger.info(f"V2从数据库获取最近{hours}小时的期权交易记录: {len(trades)}条")
                return trades
                
        except Exception as e:
            self.logger.error(f"V2获取最近期权交易记录失败: {e}")
            return []


# 全局数据库管理器实例
_db_managers = {}

def get_database_manager(market: str = 'HK') -> V2DatabaseManager:
    """获取数据库管理器单例"""
    global _db_managers
    if market not in _db_managers:
        _db_managers[market] = V2DatabaseManager(market)
    return _db_managers[market]
