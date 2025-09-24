#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用akshare更新股价脚本
支持港股、美股和A股数据获取
"""

import sys
import os
import re
import akshare as ak
from typing import Dict, Any, Optional
import time
from datetime import datetime, timedelta
import pandas as pd

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def convert_stock_code_to_akshare(stock_code: str) -> tuple[str, str]:
    """
    将内部股票代码转换为akshare格式
    
    Args:
        stock_code: 内部格式股票代码，如 'HK.00700', 'US.AAPL'
        
    Returns:
        (akshare_symbol, market_type) 元组
    """
    if stock_code.startswith('HK.'):
        hk_code = stock_code[3:]  # 去掉 'HK.'
        
        # 特殊处理指数
        if hk_code == '800000':  # 恒指
            return ('HSI', 'hk_index')
        elif hk_code == '800700':  # 科指
            return ('HSCEI', 'hk_index')
        else:
            # 港股个股，保持5位数字格式
            return (hk_code, 'hk_stock')
            
    elif stock_code.startswith('US.'):
        # 美股
        us_symbol = stock_code[3:]  # 去掉 'US.'
        return (us_symbol, 'us_stock')
    
    return (stock_code, 'unknown')

def get_hk_stock_price(symbol: str, hk_data_cache: Optional[pd.DataFrame] = None) -> Optional[float]:
    """获取港股价格 - 使用缓存的数据避免重复请求"""
    try:
        if hk_data_cache is not None:
            # 使用缓存的数据
            stock_data = hk_data_cache[hk_data_cache['代码'] == symbol]
            if not stock_data.empty:
                price = stock_data['最新价'].iloc[0]
                if pd.notna(price) and price > 0:
                    print(f"  ✓ 从缓存获取港股 {symbol} 价格: {price}")
                    return float(price)
        
        print(f"  未在缓存中找到 {symbol}")
        return None
                
    except Exception as e:
        print(f"获取港股 {symbol} 价格失败: {e}")
        return None

def get_hk_index_price(symbol: str, hk_data_cache: Optional[pd.DataFrame] = None) -> Optional[float]:
    """获取港股指数价格 - 使用缓存的数据避免重复请求"""
    try:
        if hk_data_cache is not None:
            if symbol == 'HSI':
                # 在港股数据中查找恒生指数相关代码
                possible_patterns = ['HSI', 'hsi', '恒生指数', 'Hang Seng', '800000']
                for pattern in possible_patterns:
                    hsi_data = hk_data_cache[hk_data_cache['代码'].str.contains(pattern, case=False, na=False) | 
                                hk_data_cache['名称'].str.contains(pattern, case=False, na=False)]
                    if not hsi_data.empty:
                        price = hsi_data['最新价'].iloc[0]
                        if pd.notna(price) and price > 0:
                            print(f"  ✓ 从缓存获取HSI价格: {price}")
                            return float(price)
            elif symbol == 'HSCEI':
                # 在港股数据中查找恒生科技指数相关代码
                possible_patterns = ['HSCEI', 'hscei', '科技指数', 'Tech Index', '800700']
                for pattern in possible_patterns:
                    hscei_data = hk_data_cache[hk_data_cache['代码'].str.contains(pattern, case=False, na=False) | 
                                  hk_data_cache['名称'].str.contains(pattern, case=False, na=False)]
                    if not hscei_data.empty:
                        price = hscei_data['最新价'].iloc[0]
                        if pd.notna(price) and price > 0:
                            print(f"  ✓ 从缓存获取HSCEI价格: {price}")
                            return float(price)
        
        print(f"  未在缓存中找到指数 {symbol}")
        return None
                        
    except Exception as e:
        print(f"获取港股指数 {symbol} 价格失败: {e}")
        return None

def get_us_stock_price(symbol: str, us_data_cache: Optional[pd.DataFrame] = None) -> Optional[float]:
    """获取美股价格 - 使用缓存的数据避免重复请求"""
    try:
        if us_data_cache is not None:
            # 在数据中查找对应的股票
            # 美股代码格式通常是 "105.AAPL" 或者名称匹配
            stock_row = us_data_cache[us_data_cache['代码'].str.contains(f'\.{symbol}$', na=False)]
            
            if not stock_row.empty:
                price = stock_row.iloc[0]['最新价']
                if pd.notna(price) and price > 0:
                    print(f"  ✓ 从缓存获取美股 {symbol} 价格: {price}")
                    return float(price)
            
            # 如果按代码没找到，尝试按名称查找
            symbol_name_map = {
                'AAPL': '苹果',
                'MSFT': '微软', 
                'GOOGL': '谷歌',
                'AMZN': '亚马逊',
                'TSLA': '特斯拉',
                'META': 'Meta',
                'NVDA': '英伟达',
                'NFLX': '奈飞',
                'AMD': 'AMD',
                'CRM': 'Salesforce',
                'UNH': '联合健康',
                'QS': 'QuantumScape'
            }
            
            if symbol in symbol_name_map:
                stock_row = us_data_cache[us_data_cache['名称'].str.contains(symbol_name_map[symbol], na=False)]
                if not stock_row.empty:
                    price = stock_row.iloc[0]['最新价']
                    if pd.notna(price) and price > 0:
                        print(f"  ✓ 从缓存通过名称获取美股 {symbol} 价格: {price}")
                        return float(price)
        
        print(f"  未在缓存中找到 {symbol}")
        return None
            
    except Exception as e:
        print(f"获取美股 {symbol} 价格失败: {e}")
        return None

def get_stock_price(stock_code: str, hk_cache: Optional[pd.DataFrame] = None, us_cache: Optional[pd.DataFrame] = None) -> Optional[float]:
    """
    获取股票价格的统一接口
    
    Args:
        stock_code: 内部股票代码
        hk_cache: 港股数据缓存
        us_cache: 美股数据缓存
        
    Returns:
        股票价格，获取失败返回None
    """
    akshare_symbol, market_type = convert_stock_code_to_akshare(stock_code)
    
    if market_type == 'hk_stock':
        return get_hk_stock_price(akshare_symbol, hk_cache)
    elif market_type == 'hk_index':
        return get_hk_index_price(akshare_symbol, hk_cache)
    elif market_type == 'us_stock':
        return get_us_stock_price(akshare_symbol, us_cache)
    else:
        print(f"不支持的市场类型: {market_type}")
        return None

def update_config_file(config_path: str, price_updates: Dict[str, float]) -> bool:
    """
    更新config.py文件中的股价
    
    Args:
        config_path: config.py文件路径
        price_updates: 股票代码到新价格的映射
        
    Returns:
        更新是否成功
    """
    try:
        # 读取原文件
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 备份原文件
        backup_path = f"{config_path}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with open(backup_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"已创建备份文件: {backup_path}")
        
        # 更新价格
        updated_count = 0
        for stock_code, new_price in price_updates.items():
            # 构建正则表达式匹配模式
            pattern = r"('" + re.escape(stock_code) + r"':\s*\{\s*'name':\s*'[^']*',\s*'default_price':\s*)[0-9]+\.?[0-9]*(\s*,\s*'monitor':\s*[^}]*\})"
            
            replacement = r"\g<1>" + str(new_price) + r"\g<2>"
            
            new_content = re.sub(pattern, replacement, content)
            
            if new_content != content:
                content = new_content
                updated_count += 1
                print(f"✓ 已更新 {stock_code} 价格为 {new_price}")
            else:
                print(f"✗ 未找到 {stock_code} 的配置项")
        
        # 写入更新后的内容
        if updated_count > 0:
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"\n成功更新了 {updated_count} 个股票的价格")
            return True
        else:
            print("\n没有价格需要更新")
            return False
            
    except Exception as e:
        print(f"更新配置文件失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("akshare股价更新脚本")
    print("=" * 60)
    
    # 导入配置
    try:
        from v2_system.config import STOCK_CONFIG
    except ImportError as e:
        print(f"无法导入配置: {e}")
        return
    
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'v2_system', 'config.py')
    
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        return
    
    print(f"配置文件路径: {config_path}")
    print(f"需要更新的股票数量: {len(STOCK_CONFIG)}")
    print()
    
    # 预先获取数据缓存，大幅减少API请求
    print("正在获取市场数据缓存...")
    hk_data_cache = None
    us_data_cache = None
    
    # 检查是否有港股需要更新
    hk_stocks = [code for code in STOCK_CONFIG.keys() if code.startswith('HK.')]
    if hk_stocks:
        try:
            print("  获取港股数据缓存...")
            hk_data_cache = ak.stock_hk_spot_em()
            print(f"  ✓ 成功获取港股数据，共 {len(hk_data_cache)} 只股票")
            time.sleep(2)  # 添加延迟避免限流
        except Exception as e:
            print(f"  ✗ 获取港股数据失败: {e}")
    
    # 检查是否有美股需要更新
    us_stocks = [code for code in STOCK_CONFIG.keys() if code.startswith('US.')]
    if us_stocks:
        try:
            print("  获取美股数据缓存...")
            us_data_cache = ak.stock_us_spot_em()
            print(f"  ✓ 成功获取美股数据，共 {len(us_data_cache)} 只股票")
            time.sleep(2)  # 添加延迟避免限流
        except Exception as e:
            print(f"  ✗ 获取美股数据失败: {e}")
    
    print()
    
    # 获取所有股票的最新价格
    price_updates = {}
    failed_stocks = []
    
    for i, (stock_code, config) in enumerate(STOCK_CONFIG.items(), 1):
        stock_name = config['name']
        current_price = config['default_price']
        
        print(f"[{i}/{len(STOCK_CONFIG)}] 正在获取 {stock_code} ({stock_name}) 的价格...")
        
        # 转换为akshare格式
        akshare_symbol, market_type = convert_stock_code_to_akshare(stock_code)
        print(f"  akshare代码: {akshare_symbol} (市场: {market_type})")
        
        # 获取最新价格（使用缓存数据）
        new_price = get_stock_price(stock_code, hk_data_cache, us_data_cache)
        
        if new_price is not None:
            price_updates[stock_code] = new_price
            change = new_price - current_price
            change_pct = (change / current_price) * 100 if current_price > 0 else 0
            print(f"  当前价格: {current_price} -> 最新价格: {new_price:.2f} ({change:+.2f}, {change_pct:+.2f}%)")
        else:
            failed_stocks.append((stock_code, stock_name))
            print(f"  ✗ 获取价格失败")
        
        print()
        
        # 减少延迟，因为现在使用缓存数据，不需要频繁的API请求
        if i < len(STOCK_CONFIG):
            time.sleep(0.1)
    
    # 显示汇总信息
    print("=" * 60)
    print("价格获取汇总:")
    print(f"  成功获取: {len(price_updates)} 个")
    print(f"  获取失败: {len(failed_stocks)} 个")
    
    if failed_stocks:
        print("\n获取失败的股票:")
        for stock_code, stock_name in failed_stocks:
            print(f"  - {stock_code} ({stock_name})")
    
    # 更新配置文件
    if price_updates:
        print("\n" + "=" * 60)
        print("开始更新配置文件...")
        
        success = update_config_file(config_path, price_updates)
        
        if success:
            print("✓ 配置文件更新完成!")
        else:
            print("✗ 配置文件更新失败!")
    else:
        print("\n没有获取到任何价格数据，跳过配置文件更新")
    
    print("=" * 60)

if __name__ == '__main__':
    main()