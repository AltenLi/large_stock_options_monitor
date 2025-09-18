# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
V2系统期权代码解析器 - 统一解析期权到期日、类型和行权价格
"""

import re
from datetime import datetime, date
from typing import Dict, Optional, Tuple, Union, Any
import logging

logger = logging.getLogger(__name__)


class OptionCodeParser:
    """期权代码解析器"""
    
    def __init__(self):
        # 港股期权代码正则模式
        # 实际格式: HK.TCH250919C670000 (股票简称TCH，2025年09月19日，Call，行权价67.0000)
        # 实际格式: HK.BIU250919C120000 (股票简称BIU，2025年09月19日，Call，行权价12.0000)
        # 实际格式: HK.JDC250929P122500 (股票简称JDC，2025年09月29日，Put，行权价12.2500)
        self.hk_option_patterns = [
            # 主要格式: HK.{股票简称}{YYMMDD}{C/P}{价格}
            r'HK\.([A-Z]{2,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)',
            # 备用格式: 可能的其他变体
            r'HK\.([A-Z0-9]{2,5})(\d{6})([CP])(\d+)',
        ]
        
        # 美股期权代码正则模式
        # 实际格式: US.TSLA251017P205000 (股票代码TSLA，2025年10月17日，Put，行权价205.000)
        # 实际格式: US.AAPL250920C180000 (股票代码AAPL，2025年09月20日，Call，行权价180.000)
        self.us_option_patterns = [
            # 主要格式: US.{股票代码}{YYMMDD}{C/P}{价格}
            r'US\.([A-Z]{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)',
            # 备用格式: 可能的其他变体
            r'US\.([A-Z0-9]{1,5})(\d{6})([CP])(\d+)',
        ]
    
    def parse_option_code(self, option_code: str) -> Dict[str, Any]:
        """
        解析期权代码，返回包含所有信息的字典
        
        Args:
            option_code: 期权代码，如 'HK.00700C241225'
            
        Returns:
            Dict包含:
            - stock_code: 股票代码 (如 'HK.00700')
            - option_type: 期权类型 ('Call' 或 'Put')
            - expiry_date: 到期日 (YYYY-MM-DD格式)
            - strike_price: 行权价格 (浮点数)
            - is_valid: 是否解析成功
            - raw_code: 原始代码
        """
        result = {
            'stock_code': None,
            'option_type': None,
            'expiry_date': None,
            'strike_price': None,
            'is_valid': False,
            'raw_code': option_code
        }
        
        try:
            if not option_code or not isinstance(option_code, str):
                return result
            
            option_code = option_code.strip().upper()
            
            # 尝试港股期权格式
            if option_code.startswith('HK.'):
                parsed = self._parse_hk_option(option_code)
                if parsed['is_valid']:
                    return parsed
            
            # 尝试美股期权格式
            elif option_code.startswith('US.'):
                parsed = self._parse_us_option(option_code)
                if parsed['is_valid']:
                    return parsed
            
            logger.debug(f"V2无法解析期权代码: {option_code}")
            return result
            
        except Exception as e:
            logger.error(f"V2解析期权代码异常: {option_code}, 错误: {e}")
            return result
    
    def _parse_hk_option(self, option_code: str) -> Dict[str, Any]:
        """解析港股期权代码"""
        result = {
            'stock_code': None,
            'option_type': None,
            'expiry_date': None,
            'strike_price': None,
            'is_valid': False,
            'raw_code': option_code
        }
        
        # 主要模式: HK.{股票简称}{YYMMDD}{C/P}{价格}
        # 例如: HK.TCH250919C670000
        pattern1 = r'HK\.([A-Z]{2,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)'
        match = re.match(pattern1, option_code)
        if match:
            stock_symbol = match.group(1)  # 股票简称，如TCH, BIU, JDC
            year = match.group(2)          # 年份，如25
            month = match.group(3)         # 月份，如09
            day = match.group(4)           # 日期，如19
            option_type_char = match.group(5)  # C或P
            strike_raw = match.group(6)    # 行权价格，如670000
            
            # 构建股票代码 (保持简称格式)
            result['stock_code'] = f'HK.{stock_symbol}'
            
            # 解析期权类型
            result['option_type'] = 'Call' if option_type_char == 'C' else 'Put'
            
            # 解析到期日
            try:
                # 年份处理：25 -> 2025
                full_year = 2000 + int(year)
                if full_year < 2020:  # 如果小于2020，认为是下个世纪
                    full_year += 100
                
                expiry_date = date(full_year, int(month), int(day))
                result['expiry_date'] = expiry_date.strftime('%Y-%m-%d')
            except ValueError as e:
                logger.error(f"V2解析到期日失败: {year}-{month}-{day}, 错误: {e}")
                return result
            
            # 解析行权价格
            try:
                # 根据价格数字的长度和股票类型智能判断除数
                # 对于高价股（如腾讯），680000 应该是 680.00，除以1000
                # 对于低价股，120000 可能是 12.00，除以10000
                strike_raw_int = int(strike_raw)
                
                # 根据股票简称和价格范围智能判断
                # 高价股列表（通常股价在100港币以上）
                high_price_stocks = ['TCH', 'HEX', 'MEI', 'JDC', 'ALI']  # 腾讯、港交所、美团、京东、阿里等
                # 中价股列表（通常股价在20-100港币）
                mid_price_stocks = ['BIU', 'KUA', 'ZMI']  # 小米、快手等
                
                if stock_symbol in high_price_stocks:
                    # 高价股：通常6位数除以1000，5位数除以100
                    if len(strike_raw) >= 6:
                        strike_price = float(strike_raw_int) / 1000.0
                    else:
                        strike_price = float(strike_raw_int) / 100.0
                elif stock_symbol in mid_price_stocks:
                    # 中价股：6位数可能除以10000，5位数除以1000
                    if len(strike_raw) >= 6:
                        strike_price = float(strike_raw_int) / 10000.0
                    else:
                        strike_price = float(strike_raw_int) / 1000.0
                else:
                    # 未知股票，根据数值大小智能判断
                    if len(strike_raw) >= 6:
                        if strike_raw_int >= 500000:  # 大于50万，可能是高价股
                            strike_price = float(strike_raw_int) / 1000.0
                        else:  # 小于50万，可能是低价股
                            strike_price = float(strike_raw_int) / 10000.0
                    elif len(strike_raw) >= 5:
                        if strike_raw_int >= 50000:  # 大于5万，除以1000
                            strike_price = float(strike_raw_int) / 1000.0
                        else:  # 小于5万，除以100
                            strike_price = float(strike_raw_int) / 100.0
                    else:
                        # 较短数字，除以100
                        strike_price = float(strike_raw_int) / 100.0
                
                result['strike_price'] = strike_price
                logger.debug(f"V2解析行权价格: {strike_raw} -> {strike_price} (股票: {stock_symbol})")
            except ValueError:
                logger.error(f"V2解析行权价格失败: {strike_raw}")
                return result
            
            result['is_valid'] = True
            return result
        
        return result
    
    def _parse_us_option(self, option_code: str) -> Dict[str, Any]:
        """解析美股期权代码"""
        result = {
            'stock_code': None,
            'option_type': None,
            'expiry_date': None,
            'strike_price': None,
            'is_valid': False,
            'raw_code': option_code
        }
        
        # 主要模式: US.{股票代码}{YYMMDD}{C/P}{价格}
        # 例如: US.TSLA251017P205000
        pattern1 = r'US\.([A-Z]{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)'
        match = re.match(pattern1, option_code)
        if match:
            stock_symbol = match.group(1)  # 股票代码，如TSLA, AAPL
            year = match.group(2)          # 年份，如25
            month = match.group(3)         # 月份，如10
            day = match.group(4)           # 日期，如17
            option_type_char = match.group(5)  # C或P
            strike_raw = match.group(6)    # 行权价格，如205000
            
            # 构建股票代码
            result['stock_code'] = f'US.{stock_symbol}'
            
            # 解析期权类型
            result['option_type'] = 'Call' if option_type_char == 'C' else 'Put'
            
            # 解析到期日
            try:
                # 年份处理：25 -> 2025
                full_year = 2000 + int(year)
                if full_year < 2020:  # 如果小于2020，认为是下个世纪
                    full_year += 100
                
                expiry_date = date(full_year, int(month), int(day))
                result['expiry_date'] = expiry_date.strftime('%Y-%m-%d')
            except ValueError as e:
                logger.error(f"V2解析美股期权到期日失败: {year}-{month}-{day}, 错误: {e}")
                return result
            
            # 解析行权价格 - 美股期权通常是3位小数
            try:
                strike_raw_int = int(strike_raw)
                
                # 美股期权价格格式：通常6位数表示3位小数
                # 例如：205000 = 205.000, 180000 = 180.000
                if len(strike_raw) >= 6:
                    strike_price = float(strike_raw_int) / 1000.0
                elif len(strike_raw) >= 5:
                    strike_price = float(strike_raw_int) / 1000.0
                else:
                    # 较短数字，除以100
                    strike_price = float(strike_raw_int) / 100.0
                
                result['strike_price'] = strike_price
                logger.debug(f"V2解析美股期权行权价格: {strike_raw} -> {strike_price} (股票: {stock_symbol})")
            except ValueError:
                logger.error(f"V2解析美股期权行权价格失败: {strike_raw}")
                return result
            
            result['is_valid'] = True
            return result
        
        return result
    
    def get_option_type(self, option_code: str) -> str:
        """获取期权类型"""
        parsed = self.parse_option_code(option_code)
        return parsed.get('option_type', '未知')
    
    def get_expiry_date(self, option_code: str) -> Optional[str]:
        """获取到期日"""
        parsed = self.parse_option_code(option_code)
        return parsed.get('expiry_date')
    
    def get_strike_price(self, option_code: str) -> Optional[float]:
        """获取行权价格"""
        parsed = self.parse_option_code(option_code)
        return parsed.get('strike_price')
    
    def get_stock_code(self, option_code: str) -> Optional[str]:
        """获取标的股票代码"""
        parsed = self.parse_option_code(option_code)
        return parsed.get('stock_code')


# 全局解析器实例
option_parser = OptionCodeParser()


def parse_option_code(option_code: str) -> Dict[str, Any]:
    """便捷函数：解析期权代码"""
    return option_parser.parse_option_code(option_code)


def get_option_type(option_code: str) -> str:
    """便捷函数：获取期权类型"""
    return option_parser.get_option_type(option_code)


def get_expiry_date(option_code: str) -> Optional[str]:
    """便捷函数：获取到期日"""
    return option_parser.get_expiry_date(option_code)


def get_strike_price(option_code: str) -> Optional[float]:
    """便捷函数：获取行权价格"""
    return option_parser.get_strike_price(option_code)


def get_stock_code(option_code: str) -> Optional[str]:
    """便捷函数：获取标的股票代码"""
    return option_parser.get_stock_code(option_code)
