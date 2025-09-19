# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
V2系统大单期权处理器 - 独立版本
"""

import json
import os
import logging
import pandas as pd
import time
import traceback
import re
import functools
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable, TypeVar, ParamSpec
import sys

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import HK_TRADING_HOURS, US_TRADING_HOURS_DST, US_TRADING_HOURS_STD, OPTION_FILTERS, SYSTEM_CONFIG, get_stock_name, get_stock_default_price, get_monitor_stocks
from .data_utils import safe_int_convert, safe_float_convert, safe_str_convert
import futu as ft


P = ParamSpec("P")
R = TypeVar("R")

def retry_on_api_error(max_retries: int = 3, *, delay: float = 5.0) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """API调用失败时的重试装饰器，默认重试间隔5秒"""
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            logger = logging.getLogger('V2OptionMonitor.BigOptionsProcessor')
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"API调用失败，已重试{retries}次，放弃: {e}")
                        raise
                    logger.warning(f"API调用失败，{retries}/{max_retries}次重试: {e}")
                    time.sleep(delay)  # 使用可配置的重试间隔
                    logger.info(f"正在进行第{retries}次重试...")
            # 理论上不会到这里；为安全起见最后再尝试一次
            return func(*args, **kwargs)
        return wrapper
    return decorator


def retry_api_call_with_empty_check(api_func, *args, max_retries: int = 3, delay: float = 5.0, **kwargs):
    """
    通用API调用重试函数，包含空数据检查
    
    Args:
        api_func: API调用函数
        *args: API函数的位置参数
        max_retries: 最大重试次数
        delay: 重试间隔(秒)
        **kwargs: API函数的关键字参数
    
    Returns:
        tuple: (ret_code, data) API调用结果
    
    Raises:
        Exception: 重试次数用尽后抛出最后一次的异常
    """
    logger = logging.getLogger('V2OptionMonitor.BigOptionsProcessor')
    
    for attempt in range(max_retries):
        try:
            ret, data = api_func(*args, **kwargs)
            
            # 检查API调用是否成功
            if ret != ft.RET_OK:
                raise Exception(f"API调用失败，返回码: {ret}")
            
            # 检查数据是否为空
            if hasattr(data, 'empty') and data.empty:
                raise Exception("API调用返回空数据")
            elif isinstance(data, (list, dict)) and len(data) == 0:
                raise Exception("API调用返回空数据")
            
            # 成功获取到有效数据
            logger.debug(f"API调用成功，获取到数据: {len(data) if hasattr(data, '__len__') else 'N/A'} 条记录")
            return ret, data
            
        except Exception as e:
            attempt_num = attempt + 1
            if attempt_num >= max_retries:
                logger.error(f"API调用失败，已重试{attempt_num}次，放弃: {e}")
                raise
            
            logger.warning(f"API调用失败，{attempt_num}/{max_retries}次重试: {e}")
            time.sleep(delay)
            logger.info(f"正在进行第{attempt_num}次重试...")
    
    # 理论上不会到这里
    return api_func(*args, **kwargs)


class BigOptionsProcessor:
    """V2系统大单期权处理器"""
    
    def __init__(self, market: str = 'HK'):
        self.market = market
        self.logger = logging.getLogger(f'V2OptionMonitor.BigOptionsProcessor.{market}')
        
        # 初始化数据库管理器，根据市场选择对应数据库
        from .database_manager import get_database_manager
        self.db_manager = get_database_manager(market)
        
        # 数据现在统一存储在数据库中，不再使用JSON文件
        self.stock_price_cache = {}  # 缓存股价信息
        self.price_cache_time = {}   # 缓存时间
        self.last_option_volumes = {}  # 缓存上一次的期权交易量
        self.notification_history = {}  # 通知历史，避免重复通知
        self.today_option_volumes = {}  # 当日期权成交量缓存
        self.today_volumes_loaded = False  # 是否已加载当日数据
        
        # 数据现在统一存储在数据库中，不再需要创建JSON文件目录
        # os.makedirs(os.path.dirname(self.json_file), exist_ok=True)
    
    def _get_filter_key(self, stock_code: str) -> str:
        """根据股票代码获取对应的过滤器配置键"""
        # 恒生指数期权
        if stock_code == 'HK.800000':
            return 'hsi_options'
        # 科指期权
        elif stock_code == 'HK.800700':
            return 'hscei_options'
        # 美股期权
        elif self.market == 'US':
            return 'us_default'
        # 港股其他期权
        else:
            return 'hk_default'
    
    def _load_today_option_volumes(self) -> Dict[str, int]:
        """从SQL数据库加载当日期权成交量"""
        if self.today_volumes_loaded:
            return self.today_option_volumes
        
        try:
            from .database_manager import get_database_manager
            db_manager = get_database_manager(self.market)
            
            # 从数据库获取当日所有期权的最新成交量
            self.today_option_volumes = db_manager.get_today_all_option_volumes()
            self.today_volumes_loaded = True
            
            self.logger.info(f"V2从数据库加载当日期权成交量: {len(self.today_option_volumes)}个期权")
            return self.today_option_volumes
            
        except Exception as e:
            self.logger.error(f"V2从数据库加载当日期权成交量失败: {e}")
            return {}
    

    def _update_today_volume_cache(self, option_code: str, volume: int):
        """更新当日成交量缓存"""
        try:
            # 确保已加载当日数据
            if not self.today_volumes_loaded:
                self._load_today_option_volumes()
            
            # 更新内存缓存
            self.today_option_volumes[option_code] = volume
            
        except Exception as e:
            self.logger.debug(f"V2更新{option_code}成交量缓存失败: {e}")
    
    def _save_to_database(self, trade_info: Dict[str, Any]) -> bool:
        """保存期权交易数据到SQL数据库"""
        try:
            from .database_manager import get_database_manager
            db_manager = get_database_manager(self.market)
            
            # 保存到数据库
            success = db_manager.save_option_trade(trade_info)
            
            if success:
                self.logger.debug(f"V2期权交易数据已保存到数据库: {trade_info.get('option_code')}")
            else:
                self.logger.warning(f"V2期权交易数据保存失败: {trade_info.get('option_code')}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"V2保存期权交易数据到数据库失败: {e}")
            return False
    
    def _load_stock_info_from_file(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """从V2系统文件读取单只股票信息"""
        try:
            prices_file = SYSTEM_CONFIG['price_cache']
            base_file = SYSTEM_CONFIG['stock_info_cache']

            price_val = None
            name_val = ""

            # 先尝试基础信息中的名称
            try:
                if os.path.exists(base_file):
                    with open(base_file, 'r', encoding='utf-8') as f:
                        base_data = json.load(f)
                    stocks = base_data.get('stocks') if isinstance(base_data, dict) else None
                    if isinstance(stocks, dict):
                        base_info = stocks.get(stock_code)
                        if isinstance(base_info, dict):
                            n = base_info.get('name')
                            if isinstance(n, str) and n.strip():
                                name_val = n
            except Exception:
                pass

            # 读取价格与（次级）名称
            if os.path.exists(prices_file):
                with open(prices_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                info = (data.get('prices') or {}).get(stock_code)
                if isinstance(info, dict):
                    pv = info.get('price')
                    if isinstance(pv, (int, float)):
                        price_val = float(pv)
                    # 若基础信息没有名称，则尝试从这里补充
                    n2 = info.get('name')
                    if (not name_val) and isinstance(n2, str) and n2.strip():
                        name_val = n2

            if isinstance(price_val, (int, float)):
                return {'price': float(price_val), 'name': name_val}
            return None
        except Exception:
            return None

    def get_recent_big_options(self, quote_ctx, stock_codes: List[str], option_monitor=None) -> List[Dict[str, Any]]:
        """获取最近的大单期权 - V2版本"""
        all_big_options = []
        processed_stocks = set()
        failed_stocks = set()
        
        self.logger.info(f"V2系统开始获取 {len(stock_codes)} 只股票的大单期权数据...")
        
        # 预先获取所有股票的价格
        stock_prices = self._batch_get_stock_prices(quote_ctx, stock_codes, option_monitor)
        
        for i, stock_code in enumerate(stock_codes):
            try:
                if stock_code in processed_stocks or stock_code in failed_stocks:
                    continue
                
                self.logger.info(f"V2处理 {i+1}/{len(stock_codes)}: {stock_code}")
                
                # 获取该股票的所有期权代码
                try:
                    option_codes = self._get_option_codes(quote_ctx, stock_code, option_monitor)
                except Exception as e:
                    self.logger.error(f"V2获取{stock_code}期权代码异常: {e}")
                    failed_stocks.add(stock_code)
                    continue
                
                if option_codes:
                    self.logger.info(f"V2 {stock_code} 获取到 {len(option_codes)} 个期权代码")
                    
                    # 🚀 使用批量处理优化：一次API调用处理所有期权
                    stock_big_options = []
                    
                    try:
                        # 构建期权-股票映射
                        option_stock_map = {option_code: stock_code for option_code in option_codes}
                        
                        # 批量获取期权大单交易
                        batch_big_trades = self._get_options_big_trades_batch(quote_ctx, option_codes, option_stock_map, option_monitor)
                        
                        # 对返回的大单期权进行通知过滤
                        for trade in batch_big_trades:
                            if self._should_notify(trade):
                                stock_big_options.append(trade)
                                self.logger.info(f"V2期权 {trade['option_code']} 发现大单并符合通知条件")
                            else:
                                self.logger.debug(f"V2期权 {trade['option_code']} 是大单但不符合通知条件（冷却期内）")
                        
                        self.logger.info(f"V2批量处理 {stock_code}: {len(option_codes)}个期权 -> {len(batch_big_trades)}个大单 -> {len(stock_big_options)}个通知")
                        
                    except Exception as e:
                        self.logger.error(f"V2批量处理{stock_code}期权失败，回退到单个处理: {e}")
                        
                        # 回退到单个处理模式
                        error_count = 0
                        for j, option_code in enumerate(option_codes):
                            try:
                                if error_count >= 3:
                                    self.logger.warning(f"V2连续错误超过3次，跳过{stock_code}剩余期权")
                                    break
                                    
                                option_big_trades = self._get_option_big_trades(quote_ctx, option_code, stock_code, option_monitor)
                                if option_big_trades:
                                    for trade in option_big_trades:
                                        if self._should_notify(trade):
                                            stock_big_options.append(trade)
                                            self.logger.info(f"V2期权 {j+1}/{len(option_codes)}: {option_code} 发现大单并符合通知条件")
                                        else:
                                            self.logger.debug(f"V2期权 {option_code} 是大单但不符合通知条件（冷却期内）")
                                    error_count = 0
                                
                                # 每处理5个期权暂停一下
                                if (j + 1) % 5 == 0:
                                    time.sleep(0.5)
                                    
                            except Exception as e:
                                self.logger.error(f"V2处理期权 {option_code} 失败: {e}")
                                error_count += 1
                    
                    if stock_big_options:
                        self.logger.info(f"V2 {stock_code} 发现 {len(stock_big_options)} 笔大单期权")
                        all_big_options.extend(stock_big_options)
                    else:
                        self.logger.info(f"V2 {stock_code} 未发现大单期权")
                else:
                    self.logger.warning(f"V2 {stock_code} 未获取到期权代码")
                
                processed_stocks.add(stock_code)
                time.sleep(1)  # 避免API调用过于频繁
                
            except Exception as e:
                self.logger.error(f"V2获取{stock_code}大单期权失败: {e}")
                self.logger.error(traceback.format_exc())
        
        # 按成交额降序排序
        all_big_options.sort(key=lambda x: x.get('turnover', 0), reverse=True)
        
        # 为每个期权添加正股价格和名称信息，并保存股票信息到数据库
        for option in all_big_options:
            stock_code = option.get('stock_code')
            if stock_code and stock_code in stock_prices:
                stock_info = stock_prices[stock_code]
                if isinstance(stock_info, dict):
                    option['stock_price'] = stock_info.get('price', 0)
                    option['stock_name'] = stock_info.get('name', '')
                    
                    # 保存股票信息到数据库
                    try:
                        from .database_manager import get_database_manager
                        db_manager = get_database_manager(self.market)
                        db_manager.save_stock_info(
                            stock_code=stock_code,
                            stock_name=stock_info.get('name', ''),
                            current_price=stock_info.get('price', 0)
                        )
                    except Exception as e:
                        self.logger.debug(f"V2保存股票信息到数据库失败 {stock_code}: {e}")
                else:
                    option['stock_price'] = stock_info
        
        self.logger.info(f"V2系统总共发现 {len(all_big_options)} 笔大单期权")
        
        # 打印每只股票的大单数量
        stock_counts = {}
        for option in all_big_options:
            stock_code = option.get('stock_code', 'Unknown')
            stock_counts[stock_code] = stock_counts.get(stock_code, 0) + 1
        
        for stock_code, count in stock_counts.items():
            self.logger.info(f"📊 V2 {stock_code}: {count} 笔大单")
        
        return all_big_options
    
    def _should_notify(self, trade_info: Dict[str, Any]) -> bool:
        """检查是否应该发送通知"""
        option_code = trade_info.get('option_code')
        current_time = datetime.now()
        
        # 移除通知冷却逻辑，直接更新通知历史并返回True
        self.notification_history[option_code] = current_time
        self.logger.debug(f"V2期权 {option_code} 通过通知检查，更新通知历史")
        return True
    
    @retry_on_api_error(max_retries=3)
    def _batch_get_stock_prices(self, quote_ctx, stock_codes: List[str], option_monitor=None) -> Dict[str, Dict[str, Any]]:
        """V2系统批量获取股票价格和名称"""
        result = {}
        current_time = datetime.now()
        
        # 如果提供了option_monitor实例，优先使用其股价缓存
        if option_monitor and hasattr(option_monitor, 'stock_price_cache'):
            self.logger.info(f"V2使用option_monitor中的股价缓存")
            
            for stock_code in stock_codes:
                if stock_code in option_monitor.stock_price_cache:
                    price_obj = option_monitor.stock_price_cache[stock_code]

                    actual_price = None
                    name_from_monitor = ""
                    if isinstance(price_obj, dict):
                        try:
                            pv = price_obj.get('price')
                            if isinstance(pv, (int, float)):
                                actual_price = float(pv)
                            name_from_monitor = price_obj.get('name', '') or ""
                        except Exception:
                            actual_price = None
                    else:
                        if isinstance(price_obj, (int, float)):
                            actual_price = float(price_obj)
                            # 当price_obj是数字时，尝试从本地缓存获取名称
                            if stock_code in self.stock_price_cache and isinstance(self.stock_price_cache[stock_code], dict):
                                name_from_monitor = self.stock_price_cache[stock_code].get('name', '') or ""

                    stock_info = {
                        'price': float(actual_price) if isinstance(actual_price, (int, float)) else 0.0,
                        'name': name_from_monitor
                    }

                    # 如果没有名称，尝试从文件缓存补齐
                    if not stock_info['name']:
                        file_info = self._load_stock_info_from_file(stock_code)
                        if file_info and file_info.get('name'):
                            stock_info['name'] = file_info['name']

                    result[stock_code] = stock_info
                    self.stock_price_cache[stock_code] = stock_info
                    self.price_cache_time[stock_code] = current_time
                    self.logger.debug(f"V2从option_monitor获取股价: {stock_code} = {stock_info['price']}")
                else:
                    # 检查本地缓存
                    if stock_code in self.stock_price_cache and stock_code in self.price_cache_time:
                        if (current_time - self.price_cache_time[stock_code]).seconds < 300:
                            result[stock_code] = self.stock_price_cache[stock_code]
                            continue
        else:
            # 检查哪些股票需要更新价格
            for stock_code in stock_codes:
                if stock_code in self.stock_price_cache and stock_code in self.price_cache_time:
                    if (current_time - self.price_cache_time[stock_code]).seconds < 300:
                        result[stock_code] = self.stock_price_cache[stock_code]
                        continue
        
        # 找出仍需要更新的股票
        stocks_to_update = [code for code in stock_codes if code not in result]
        
        if not stocks_to_update:
            self.logger.info("V2所有股价都已获取，无需更新")
            return result
        
        # 批量获取股价和名称
        try:
            self.logger.info(f"V2批量获取 {len(stocks_to_update)} 只股票的价格和名称...")
            ret, data = retry_api_call_with_empty_check(
                quote_ctx.get_market_snapshot,
                stocks_to_update,
                max_retries=3,
                delay=10.0
            )
            if ret == ft.RET_OK and not data.empty:
                for _, row in data.iterrows():
                    code = row['code']
                    price = float(row['last_price'])
                    name = row.get('name', '') or row.get('stock_name', '')
                    
                    stock_info = {
                        'price': price,
                        'name': name
                    }
                    
                    result[code] = stock_info
                    self.stock_price_cache[code] = stock_info
                    self.price_cache_time[code] = current_time
                    self.logger.debug(f"V2获取股票信息: {code} = {price} ({name})")
                        
                    # 保存股票信息到数据库
                    try:
                        from .database_manager import get_database_manager
                        db_manager = get_database_manager(self.market)
                        db_manager.save_stock_info(
                            stock_code=code,
                            stock_name=name,
                            current_price=price
                        )
                    except Exception as e:
                        self.logger.warning(f"V2保存股票信息到数据库失败 {code}: {e}")
                
                    self.logger.info(f"V2成功获取 {len(data)} 只股票的价格和名称")
            else:
                self.logger.warning(f"V2批量获取股票信息失败: {ret}")
                # 使用缓存中的旧数据
                for stock_code in stocks_to_update:
                    if stock_code in self.stock_price_cache:
                        result[stock_code] = self.stock_price_cache[stock_code]
                    else:
                        # 使用默认价格和config.py中的get_stock_name函数获取股票名称
                        default_price = get_stock_default_price(stock_code)
                        stock_name = get_stock_name(stock_code)
                        result[stock_code] = {'price': default_price, 'name': stock_name}
                        self.logger.warning(f"V2API调用失败时使用get_stock_name获取股票名称: {stock_code} = {stock_name}")
        
        except Exception as e:
            self.logger.error(f"V2批量获取股票信息异常: {e}")
            # 使用缓存中的旧数据
            for stock_code in stocks_to_update:
                    if stock_code in self.stock_price_cache:
                        result[stock_code] = self.stock_price_cache[stock_code]
                    else:
                        # 使用默认价格和config.py中的get_stock_name函数获取股票名称
                        default_price = get_stock_default_price(stock_code)
                        stock_name = get_stock_name(stock_code)
                        result[stock_code] = {'price': default_price, 'name': stock_name}
                        self.logger.warning(f"V2API调用失败时使用get_stock_name获取股票名称: {stock_code} = {stock_name}")
        
        return result
    
    @retry_on_api_error(max_retries=3)
    def _get_option_codes(self, quote_ctx, stock_code: str, option_monitor=None) -> List[str]:
        """V2系统获取期权代码列表 - 支持港股和美股"""
        try:
            option_codes = []
            
            # 判断市场类型
            from config import get_market_type
            market_type = get_market_type(stock_code)
            self.logger.info(f"V2 {stock_code} 市场类型: {market_type}")
            
            # 获取当前股价
            try:
                current_price = None
                
                if option_monitor is not None:
                    stock_info = option_monitor.get_stock_price(stock_code)
                    if isinstance(stock_info, (int, float)):
                        current_price = float(stock_info)
                        self.logger.info(f"V2 {stock_code}当前股价(来自缓存): {current_price}")
                    elif isinstance(stock_info, dict) and stock_info.get('price'):
                        current_price = float(stock_info['price'])
                        self.logger.info(f"V2 {stock_code}当前股价(来自缓存): {current_price}")
                
                if current_price is None or current_price <= 0:
                    file_info = self._load_stock_info_from_file(stock_code)
                    if file_info and file_info.get('price'):
                        current_price = float(file_info['price'])
                        self.logger.info(f"V2 {stock_code}当前股价(来自文件缓存): {current_price}")
                    else:
                        # 使用默认价格
                        current_price = get_stock_default_price(stock_code)
                        self.logger.info(f"V2 {stock_code}当前股价(使用默认价格): {current_price}")
                
                # 基于股价设定期权执行价格过滤范围
                # 根据股票代码选择对应的过滤器
                filter_key = self._get_filter_key(stock_code)
                price_range = OPTION_FILTERS[filter_key].get('price_range', 0.4)
                price_lower = current_price * (1 - price_range)
                price_upper = current_price * (1 + price_range)
                self.logger.info(f"V2筛选价格范围: {price_lower:.2f} - {price_upper:.2f} (±{price_range*100}%)")
            except Exception as e:
                self.logger.error(f"V2获取{stock_code}当前股价失败: {e}")
                current_price = 100.0
                price_range = 0.5
                price_lower = current_price * (1 - price_range)
                price_upper = current_price * (1 + price_range)
            
            # 获取期权到期日 - 支持港股和美股
            try:
                ret, expiry_data = retry_api_call_with_empty_check(
                    quote_ctx.get_option_expiration_date,
                    stock_code,
                    max_retries=3,
                    delay=10.0
                )

                if ret != ft.RET_OK or expiry_data.empty:
                    self.logger.warning(f"V2 {stock_code}({market_type})没有期权合约或API调用失败")
                    return []

                # 根据市场类型调整时间范围
                now = datetime.now()
                if market_type == 'US':
                    # 美股期权通常有更多到期日，可以选择更近的
                    time_range_days = 30  # 1个月
                else:
                    # 港股期权
                    time_range_days = 30  # 1个月
                
                time_limit = now + timedelta(days=time_range_days)
                
                valid_dates = []
                for _, row in expiry_data.iterrows():
                    expiry = row['strike_time']
                    if isinstance(expiry, str):
                        try:
                            expiry = datetime.strptime(expiry, '%Y-%m-%d')
                        except:
                            continue
                    
                    if isinstance(expiry, pd.Timestamp):
                        expiry = expiry.to_pydatetime()
                    
                    if now <= expiry <= time_limit:
                        valid_dates.append(row)
                
                recent_dates = pd.DataFrame(valid_dates) if valid_dates else expiry_data.head(3)
                self.logger.info(f"V2 {stock_code}({market_type}) 找到 {len(expiry_data)} 个到期日，筛选出 {len(recent_dates)} 个{time_range_days}天内的到期日")
                
                for _, row in recent_dates.iterrows():
                    try:
                        expiry_date = row['strike_time']
                        
                        date_str = expiry_date
                        if isinstance(expiry_date, pd.Timestamp):
                            date_str = expiry_date.strftime('%Y-%m-%d')
                        elif isinstance(expiry_date, datetime):
                            date_str = expiry_date.strftime('%Y-%m-%d')
                        
                        self.logger.debug(f"V2获取 {stock_code}({market_type}) {date_str} 的期权链")
                        
                        # 使用新的重试函数获取期权链数据
                        try:
                            ret2, option_data = retry_api_call_with_empty_check(
                                quote_ctx.get_option_chain,
                                code=stock_code, 
                                start=date_str, 
                                end=date_str,
                                option_type=ft.OptionType.ALL,
                                option_cond_type=ft.OptionCondType.ALL,
                                max_retries=3,
                                delay=10.0
                            )
                            self.logger.info(f"V2 API调用成功: {stock_code}({market_type}) {expiry_date}, 获取到 {len(option_data)} 个期权")
                        except Exception as e:
                            self.logger.warning(f"V2 API调用失败: {stock_code}({market_type}) {expiry_date}, 错误: {e}")
                            continue  # 跳过这个到期日，继续处理下一个
                        
                        time.sleep(1)  # 避免API限流
                        
                        if ret2 == ft.RET_OK and not option_data.empty:
                            # 筛选执行价格在当前股价上下范围内的期权
                            filtered_options = option_data[
                                (option_data['strike_price'] >= price_lower) & 
                                (option_data['strike_price'] <= price_upper)
                            ]
                            
                            if not filtered_options.empty:
                                option_codes.extend(filtered_options['code'].tolist())
                                self.logger.info(f"V2 {stock_code}({market_type}) {expiry_date}到期的期权中有{len(filtered_options)}个在价格范围内")
                            else:
                                # 如果没有在范围内的期权，尝试放宽范围
                                wider_range = price_range * 1.5
                                wider_lower = current_price * (1 - wider_range)
                                wider_upper = current_price * (1 + wider_range)
                                
                                wider_filtered = option_data[
                                    (option_data['strike_price'] >= wider_lower) & 
                                    (option_data['strike_price'] <= wider_upper)
                                ]
                                
                                if not wider_filtered.empty:
                                    wider_filtered = wider_filtered.copy()
                                    wider_filtered.loc[:, 'price_diff'] = abs(wider_filtered['strike_price'] - current_price)
                                    closest_options = wider_filtered.nsmallest(5, 'price_diff')
                                    option_codes.extend(closest_options['code'].tolist())
                                    self.logger.info(f"V2使用更宽范围添加 {len(closest_options)} 个最接近当前价格的期权")
                    except Exception as e:
                        self.logger.warning(f"V2获取{stock_code}({market_type})期权链失败: {e}")
                        continue
                
            except Exception as e:
                self.logger.error(f"V2获取{stock_code}({market_type})期权到期日失败: {e}")
                return []
            
            if option_codes:
                self.logger.info(f"V2 {stock_code}获取到{len(option_codes)}个期权合约")
            else:
                self.logger.error(f"V2 {stock_code}未找到期权合约")
            
            return option_codes
            
        except Exception as e:
            self.logger.error(f"V2获取{stock_code}期权代码失败: {e}")
            return []
    
    @retry_on_api_error(max_retries=3)
    def _get_option_big_trades(self, quote_ctx, option_code: str, stock_code: str, option_monitor=None) -> List[Dict[str, Any]]:
        """V2系统获取期权大单交易 - 单个期权版本（保持兼容性）"""
        return self._get_options_big_trades_batch(quote_ctx, [option_code], {option_code: stock_code}, option_monitor)
    
    @retry_on_api_error(max_retries=3)
    def _get_options_big_trades_batch(self, quote_ctx, option_codes: List[str], option_stock_map: Dict[str, str], option_monitor=None) -> List[Dict[str, Any]]:
        """V2系统批量获取期权大单交易 - 使用get_market_snapshot优化"""
        try:
            if not option_codes:
                return []
            
            big_trades = []
            
            # 🚀 批量获取期权市场快照 - 一次API调用获取所有期权数据
            self.logger.info(f"V2批量获取{len(option_codes)}个期权的市场快照")
            
            try:
                ret, snapshot_data = retry_api_call_with_empty_check(
                    quote_ctx.get_market_snapshot,
                    option_codes,
                    max_retries=3,
                    delay=10.0
                )
                self.logger.info(f"V2批量获取期权快照成功，获取到 {len(snapshot_data)} 条数据")
            except Exception as e:
                self.logger.warning(f"V2批量获取期权快照失败: {e}")
                return []
            
            # 获取相关股票价格（批量获取）
            unique_stocks = list(set(option_stock_map.values()))
            stock_prices = {}
            stock_names = {}
            
            if option_monitor and hasattr(option_monitor, 'stock_price_cache'):
                # 使用监控器的股价缓存
                for stock_code in unique_stocks:
                    if stock_code in option_monitor.stock_price_cache:
                        stock_prices[stock_code] = option_monitor.stock_price_cache[stock_code]
                        if stock_code in self.stock_price_cache and isinstance(self.stock_price_cache[stock_code], dict):
                            stock_names[stock_code] = self.stock_price_cache[stock_code].get('name', '')
            else:
                # 批量获取股票快照
                try:
                    ret_stock, stock_data = retry_api_call_with_empty_check(
                        quote_ctx.get_market_snapshot,
                        unique_stocks,
                        max_retries=3,
                        delay=10.0
                    )
                    for _, row in stock_data.iterrows():
                        stock_code = row['code']
                        stock_prices[stock_code] = float(row.get('last_price', 0))
                        stock_names[stock_code] = row.get('name', get_stock_name(stock_code))
                except Exception as e:
                    self.logger.warning(f"V2批量获取股票价格失败: {e}")
                    # 使用默认价格
                    for stock_code in unique_stocks:
                        stock_prices[stock_code] = get_stock_default_price(stock_code)
                        stock_names[stock_code] = get_stock_name(stock_code)
            
            # 批量获取历史成交量和未平仓合约数数据
            option_previous_volumes = {}
            option_previous_open_interests = {}
            for option_code in option_codes:
                try:
                    # 从快照数据中获取当前数据
                    option_row = snapshot_data[snapshot_data['code'] == option_code]
                    if not option_row.empty:
                        current_volume = safe_int_convert(option_row.iloc[0].get('volume', 0))
                        current_open_interest = safe_int_convert(option_row.iloc[0].get('option_open_interest', 0))
                        
                        # 获取历史成交量
                        previous_volume = self.db_manager.get_previous_option_volume(option_code, current_volume)
                        option_previous_volumes[option_code] = previous_volume
                        
                        # 获取历史未平仓合约数
                        previous_open_interest, previous_net_open_interest = self.db_manager.get_previous_option_open_interest(option_code, current_open_interest)
                        option_previous_open_interests[option_code] = (previous_open_interest, previous_net_open_interest)
                except Exception as e:
                    self.logger.debug(f"V2获取{option_code}历史数据失败: {e}")
                    option_previous_volumes[option_code] = 0
                    option_previous_open_interests[option_code] = (0, 0)
            
            # 🔥 新逻辑：先保存所有期权数据，再筛选大单
            all_options_data = []  # 保存所有期权数据
            
            # 处理每个期权的快照数据
            for _, row in snapshot_data.iterrows():
                try:
                    option_code = row['code']
                    stock_code = option_stock_map.get(option_code, '')
                    
                    if not stock_code:
                        continue
                    
                    # 从API快照数据中获取所有需要的字段
                    # 使用安全转换函数处理可能的N/A值

                    current_volume = safe_int_convert(row.get('volume', 0))
                    current_turnover = safe_float_convert(row.get('turnover', 0))
                    last_price = safe_float_convert(row.get('last_price', 0))
                    change_rate = safe_float_convert(row.get('change_rate', 0))
                    update_time = str(row.get('update_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                    
                    # 获取未平仓合约数（新增字段）- 安全转换，处理N/A值
                    current_open_interest = safe_int_convert(row.get('option_open_interest', 0))
                    current_net_open_interest = safe_int_convert(row.get('option_net_open_interest', 0))
                    
                    # 🔥 修改：保存所有有成交量的期权，不只是大单
                    if current_volume <= 0:
                        self.logger.debug(f"V2跳过成交量为0的期权: {option_code}")
                        continue
                    
                    # 获取期权相关信息（优先使用API返回的数据）
                    api_strike_price = row.get('option_strike_price', 0) or row.get('strike_price', 0)
                    api_option_type = row.get('option_type', '')
                    api_expiry_date = row.get('option_expiry_date_distance', 0)  # 距离到期天数
                    
                    # 解析期权基本信息
                    if api_strike_price and api_strike_price > 0:
                        strike_price = float(api_strike_price)
                        # 转换期权类型
                        if hasattr(api_option_type, 'name'):
                            option_type = 'Call' if 'CALL' in str(api_option_type.name).upper() else 'Put'
                        else:
                            option_type = 'Call' if 'CALL' in str(api_option_type).upper() else 'Put'
                        expiry_date = ''  # API返回的是天数，需要计算具体日期
                    else:
                        # 使用代码解析
                        strike_price, option_type, expiry_date = self._parse_option_info_from_code(option_code)
                    
                    # 获取股票信息
                    current_stock_price = stock_prices.get(stock_code, 0)
                    stock_name = stock_names.get(stock_code, get_stock_name(stock_code))
                    
                    # 计算价格差异
                    price_diff = strike_price - current_stock_price if current_stock_price else 0
                    price_diff_pct = (price_diff / current_stock_price) * 100 if current_stock_price else 0
                    
                    # 获取历史成交量和未平仓合约数
                    previous_volume = option_previous_volumes.get(option_code, 0)
                    volume_diff = current_volume - previous_volume
                    
                    # 获取历史未平仓合约数并计算变化
                    previous_open_interest, previous_net_open_interest = option_previous_open_interests.get(option_code, (0, 0))
                    open_interest_diff = current_open_interest - previous_open_interest
                    net_open_interest_diff = current_net_open_interest - previous_net_open_interest
                    
                    # 更新当日成交量缓存
                    self._update_today_volume_cache(option_code, current_volume)
                    
                    # 构建期权交易信息
                    trade_info = {
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'option_code': option_code,
                        'timestamp': datetime.now().isoformat(),
                        'time_full': update_time,
                        'price': last_price,
                        'volume': current_volume,
                        'turnover': current_turnover,
                        'change_rate': change_rate,
                        'detected_time': datetime.now().isoformat(),
                        'data_type': 'v2_batch',
                        'strike_price': strike_price,
                        'option_type': option_type,
                        'expiry_date': expiry_date,
                        'stock_price': current_stock_price,
                        'price_diff': price_diff,
                        'price_diff_pct': price_diff_pct,
                        'volume_diff': volume_diff,
                        'last_volume': previous_volume,
                        'option_open_interest': current_open_interest,
                        'option_net_open_interest': current_net_open_interest,
                        'open_interest_diff': open_interest_diff,
                        'net_open_interest_diff': net_open_interest_diff,
                        'direction': 'Unknown'  # 批量模式下暂不获取方向信息
                    }
                    
                    # 🔥 新逻辑：保存所有期权数据到数组中
                    all_options_data.append(trade_info)
                    
                    # 保存到数据库（保存所有有成交量的期权）
                    self._save_to_database(trade_info)
                    self.logger.debug(f"V2期权数据已保存: {option_code} (成交量:{current_volume}, diff:{volume_diff}, 成交额:{current_turnover:.0f})")
                
                except Exception as e:
                    self.logger.error(f"V2处理期权{option_code}快照数据失败: {e}")
                    continue
            
            # 🔥 新逻辑：从所有期权数据中筛选出大单和有变化的期权
            for trade_info in all_options_data:
                option_code = trade_info['option_code']
                stock_code = trade_info['stock_code']  # 从trade_info中获取stock_code
                current_volume = trade_info['volume']
                current_turnover = trade_info['turnover']
                volume_diff = trade_info['volume_diff']
                
                # 检查是否满足大单条件 - 根据股票代码使用相应的过滤配置
                filter_key = self._get_filter_key(stock_code)
                option_filter = OPTION_FILTERS[filter_key]
                
                # 🔥 修改大单判断逻辑：保持原有的变化量阈值判断
                is_big_trade = (
                    current_volume >= option_filter['min_volume'] and 
                    current_turnover >= option_filter['min_turnover'] and
                    volume_diff >= option_filter['min_volume_diff']  # 保持原有的变化量阈值
                )
                
                if is_big_trade:
                    big_trades.append(trade_info)
                    
                    self.logger.info(f"🔥 V2发现大单期权: {option_code}")
                    self.logger.info(f"   执行价格: {trade_info['strike_price']:.2f}, 类型: {trade_info['option_type']}")
                    self.logger.info(f"   成交量: {current_volume:,}张, 成交额: {current_turnover:,.0f}")
                    self.logger.info(f"   变化量: +{volume_diff:,}张")
                    self.logger.info(f"   股票: {trade_info['stock_name']}({trade_info['stock_code']}), 股价: {trade_info['stock_price']:.2f}")
            
            self.logger.info(f"V2批量处理完成: {len(option_codes)}个期权, 保存{len(all_options_data)}个有成交量期权, {len(big_trades)}个大单")
            return big_trades
            
        except Exception as e:
            self.logger.error(f"V2批量获取期权大单交易失败: {e}")
            return []
    
    def _calculate_statistics(self, big_options: List[Dict[str, Any]]) -> Dict[str, Any]:
        """V2系统计算统计信息"""
        if not big_options:
            return {}
        
        df = pd.DataFrame(big_options)
        
        stats = {
            'total_volume': int(df['volume'].sum()),
            'total_turnover': float(df['turnover'].sum()),
            'avg_volume': float(df['volume'].mean()),
            'avg_turnover': float(df['turnover'].mean()),
            'unique_stocks': int(df['stock_code'].nunique()),
            'unique_options': int(df['option_code'].nunique()),
        }
        
        # 按股票分组统计
        stock_stats = df.groupby('stock_code').agg({
            'volume': 'sum',
            'turnover': 'sum',
            'option_code': 'count'
        })
        
        stock_dict = {}
        for stock in stock_stats.index:
            stock_dict[str(stock)] = {
                'volume': int(stock_stats.loc[stock, 'volume']),
                'turnover': float(stock_stats.loc[stock, 'turnover']),
                'trade_count': int(stock_stats.loc[stock, 'option_code'])
            }
        
        stats['by_stock'] = stock_dict
        
        return stats
    
    def load_current_summary(self) -> Optional[Dict[str, Any]]:
        """V2系统加载当前的汇总数据"""
        try:
            # 数据现在从数据库读取，不再从JSON文件读取
            # with open(self.json_file, 'r', encoding='utf-8') as f:
            #     return json.load(f)
            return {}  # 返回空字典，因为数据现在存储在数据库中
        except FileNotFoundError:
            return None
        except Exception as e:
            self.logger.error(f"V2加载汇总数据失败: {e}")
            return None
    
    def _parse_option_info_from_code(self, option_code: str) -> tuple:
        """从期权代码统一解析所有信息
        
        Args:
            option_code: 期权代码，格式如 HK.TCH250919C650000 或 US.AAPL250926C155000
            
        Returns:
            tuple: (strike_price, option_type, expiry_date)
        """
        try:
            # 使用统一的期权代码解析器
            from .option_code_parser import option_parser
            
            result = option_parser.parse_option_code(option_code)
            
            if result['is_valid']:
                strike_price = result['strike_price']
                option_type = result['option_type']
                expiry_date = result['expiry_date']
                
                self.logger.debug(f"V2统一解析期权代码: {option_code} -> 执行价:{strike_price}, 类型:{option_type}, 到期:{expiry_date}")
                return strike_price, option_type, expiry_date
            else:
                self.logger.warning(f"期权代码格式不匹配: {option_code}")
                return 0.0, 'Unknown', ''
                
        except Exception as e:
            self.logger.warning(f"统一解析期权代码失败 {option_code}: {e}")
            return 0.0, 'Unknown', ''
