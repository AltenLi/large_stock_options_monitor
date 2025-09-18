#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
导入股票名称到数据库的脚本
支持从JSONL文件导入港股和美股的股票代码和名称

使用方法:
python import_stock_names.py input_file.jsonl [--market HK|US]

JSONL文件格式示例:
{"stock_code": "HK.00700", "stock_name": "腾讯控股"}
{"stock_code": "US.AAPL", "stock_name": "苹果"}
"""

import os
import sys
import json
import logging
import argparse
from typing import Dict, List, Any
import sqlite3

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database_manager import get_database_manager
from config import get_market_type

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("StockNameImporter")

def read_jsonl_file(file_path: str) -> List[Dict[str, Any]]:
    """读取JSONL文件"""
    logger = logging.getLogger("StockNameImporter")
    records = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                try:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        logger.warning(f"第{line_num}行不是有效的JSON对象: {line}")
                        continue
                    
                    # 验证必要字段
                    if 'stock_code' not in record:
                        logger.warning(f"第{line_num}行缺少stock_code字段: {line}")
                        continue
                    
                    if 'stock_name' not in record:
                        logger.warning(f"第{line_num}行缺少stock_name字段: {line}")
                        continue
                    
                    records.append(record)
                except json.JSONDecodeError as e:
                    logger.warning(f"第{line_num}行JSON解析错误: {e}")
                    continue
        
        logger.info(f"从{file_path}读取了{len(records)}条记录")
        return records
    
    except Exception as e:
        logger.error(f"读取文件{file_path}失败: {e}")
        return []

def import_stock_names(records: List[Dict[str, Any]], market: str = None):
    """导入股票名称到数据库"""
    logger = logging.getLogger("StockNameImporter")
    
    # 按市场分组记录
    hk_records = []
    us_records = []
    unknown_records = []
    
    for record in records:
        stock_code = record.get('stock_code')
        if not stock_code:
            continue
        
        # 如果未指定市场，则根据股票代码自动判断
        record_market = market or get_market_type(stock_code)
        
        if record_market == 'HK':
            hk_records.append(record)
        elif record_market == 'US':
            us_records.append(record)
        else:
            unknown_records.append(record)
    
    # 导入港股数据
    if hk_records:
        db_manager = get_database_manager('HK')
        success_count = 0
        
        for record in hk_records:
            stock_code = record.get('stock_code')
            stock_name = record.get('stock_name')
            current_price = record.get('current_price')
            
            # 其他可选字段
            extra_fields = {
                'market_cap': record.get('market_cap'),
                'lot_size': record.get('lot_size'),
                'currency': record.get('currency', 'HKD')
            }
            
            success = db_manager.save_stock_info(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=current_price,
                **extra_fields
            )
            
            if success:
                success_count += 1
        
        logger.info(f"成功导入{success_count}/{len(hk_records)}条港股记录")
    
    # 导入美股数据
    if us_records:
        db_manager = get_database_manager('US')
        success_count = 0
        
        for record in us_records:
            stock_code = record.get('stock_code')
            stock_name = record.get('stock_name')
            current_price = record.get('current_price')
            
            # 其他可选字段
            extra_fields = {
                'market_cap': record.get('market_cap'),
                'lot_size': record.get('lot_size'),
                'currency': record.get('currency', 'USD')
            }
            
            success = db_manager.save_stock_info(
                stock_code=stock_code,
                stock_name=stock_name,
                current_price=current_price,
                **extra_fields
            )
            
            if success:
                success_count += 1
        
        logger.info(f"成功导入{success_count}/{len(us_records)}条美股记录")
    
    # 处理未知市场的记录
    if unknown_records:
        logger.warning(f"有{len(unknown_records)}条记录的市场类型无法识别，将被忽略")
        for record in unknown_records:
            logger.warning(f"  - {record.get('stock_code')}: {record.get('stock_name')}")

def batch_import_stock_names(records: List[Dict[str, Any]], market: str = None):
    """批量导入股票名称到数据库"""
    logger = logging.getLogger("StockNameImporter")
    
    # 按市场分组记录
    hk_records = []
    us_records = []
    unknown_records = []
    
    for record in records:
        stock_code = record.get('stock_code')
        if not stock_code:
            continue
        
        # 如果未指定市场，则根据股票代码自动判断
        record_market = market or get_market_type(stock_code)
        
        if record_market == 'HK':
            hk_records.append(record)
        elif record_market == 'US':
            us_records.append(record)
        else:
            unknown_records.append(record)
    
    # 批量导入港股数据
    if hk_records:
        db_manager = get_database_manager('HK')
        success = db_manager.batch_save_stock_info(hk_records)
        if success:
            logger.info(f"成功批量导入{len(hk_records)}条港股记录")
        else:
            logger.error(f"批量导入港股记录失败")
    
    # 批量导入美股数据
    if us_records:
        db_manager = get_database_manager('US')
        success = db_manager.batch_save_stock_info(us_records)
        if success:
            logger.info(f"成功批量导入{len(us_records)}条美股记录")
        else:
            logger.error(f"批量导入美股记录失败")
    
    # 处理未知市场的记录
    if unknown_records:
        logger.warning(f"有{len(unknown_records)}条记录的市场类型无法识别，将被忽略")
        for record in unknown_records:
            logger.warning(f"  - {record.get('stock_code')}: {record.get('stock_name')}")

def verify_import(records: List[Dict[str, Any]]):
    """验证导入结果"""
    logger = logging.getLogger("StockNameImporter")
    
    hk_db = get_database_manager('HK')
    us_db = get_database_manager('US')
    
    success_count = 0
    failed_count = 0
    
    for record in records:
        stock_code = record.get('stock_code')
        expected_name = record.get('stock_name')
        
        if stock_code.startswith('HK.'):
            info = hk_db.get_stock_info(stock_code)
        elif stock_code.startswith('US.'):
            info = us_db.get_stock_info(stock_code)
        else:
            logger.warning(f"无法验证未知市场的股票: {stock_code}")
            continue
        
        if info and info.get('stock_name') == expected_name:
            success_count += 1
        else:
            failed_count += 1
            actual_name = info.get('stock_name') if info else None
            logger.warning(f"验证失败: {stock_code} 期望名称={expected_name}, 实际名称={actual_name}")
    
    logger.info(f"验证结果: 成功={success_count}, 失败={failed_count}")

def export_current_stock_names(output_file: str):
    """导出当前数据库中的所有股票名称到JSONL文件"""
    logger = logging.getLogger("StockNameImporter")
    
    hk_db = get_database_manager('HK')
    us_db = get_database_manager('US')
    
    hk_stocks = hk_db.get_all_stock_info()
    us_stocks = us_db.get_all_stock_info()
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入港股数据
            for stock_code, info in hk_stocks.items():
                record = {
                    'stock_code': stock_code,
                    'stock_name': info.get('stock_name', ''),
                    'current_price': info.get('current_price'),
                    'market_cap': info.get('market_cap'),
                    'lot_size': info.get('lot_size'),
                    'currency': info.get('currency', 'HKD')
                }
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
            
            # 写入美股数据
            for stock_code, info in us_stocks.items():
                record = {
                    'stock_code': stock_code,
                    'stock_name': info.get('stock_name', ''),
                    'current_price': info.get('current_price'),
                    'market_cap': info.get('market_cap'),
                    'lot_size': info.get('lot_size'),
                    'currency': info.get('currency', 'USD')
                }
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        logger.info(f"成功导出{len(hk_stocks) + len(us_stocks)}条股票记录到{output_file}")
        logger.info(f"  - 港股: {len(hk_stocks)}条")
        logger.info(f"  - 美股: {len(us_stocks)}条")
        
    except Exception as e:
        logger.error(f"导出股票名称失败: {e}")

def create_example_jsonl(output_file: str):
    """创建示例JSONL文件"""
    logger = logging.getLogger("StockNameImporter")
    
    example_data = [
        {"stock_code": "HK.00700", "stock_name": "腾讯控股", "current_price": 643.0, "currency": "HKD"},
        {"stock_code": "HK.09988", "stock_name": "阿里巴巴", "current_price": 151.0, "currency": "HKD"},
        {"stock_code": "HK.03690", "stock_name": "美团", "current_price": 96.5, "currency": "HKD"},
        {"stock_code": "HK.01810", "stock_name": "小米集团", "current_price": 55.1, "currency": "HKD"},
        {"stock_code": "HK.09618", "stock_name": "京东集团", "current_price": 131.7, "currency": "HKD"},
        {"stock_code": "HK.02318", "stock_name": "中国平安", "current_price": 57.1, "currency": "HKD"},
        {"stock_code": "HK.00388", "stock_name": "香港交易所", "current_price": 448.4, "currency": "HKD"},
        {"stock_code": "HK.00981", "stock_name": "中芯国际", "current_price": 63.0, "currency": "HKD"},
        {"stock_code": "HK.02020", "stock_name": "安踏体育", "current_price": 93.0, "currency": "HKD"},
        {"stock_code": "HK.01024", "stock_name": "快手", "current_price": 75.0, "currency": "HKD"},
        {"stock_code": "US.AAPL", "stock_name": "苹果", "current_price": 234.0, "currency": "USD"},
        {"stock_code": "US.MSFT", "stock_name": "微软", "current_price": 509.0, "currency": "USD"},
        {"stock_code": "US.GOOGL", "stock_name": "谷歌", "current_price": 241.0, "currency": "USD"},
        {"stock_code": "US.AMZN", "stock_name": "亚马逊", "current_price": 229.0, "currency": "USD"},
        {"stock_code": "US.TSLA", "stock_name": "特斯拉", "current_price": 396.0, "currency": "USD"},
        {"stock_code": "US.META", "stock_name": "Meta", "current_price": 755.0, "currency": "USD"},
        {"stock_code": "US.NVDA", "stock_name": "英伟达", "current_price": 117.5, "currency": "USD"},
        {"stock_code": "US.NFLX", "stock_name": "奈飞", "current_price": 1188.0, "currency": "USD"},
        {"stock_code": "US.AMD", "stock_name": "AMD", "current_price": 158.0, "currency": "USD"},
        {"stock_code": "US.CRM", "stock_name": "Salesforce", "current_price": 242.7, "currency": "USD"}
    ]
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("# 股票名称导入示例文件\n")
            f.write("# 每行一个JSON对象，包含stock_code和stock_name字段\n")
            f.write("# 可选字段: current_price, market_cap, lot_size, currency\n\n")
            
            for record in example_data:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
        
        logger.info(f"成功创建示例文件: {output_file}")
        
    except Exception as e:
        logger.error(f"创建示例文件失败: {e}")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='导入股票名称到数据库')
    parser.add_argument('input_file', nargs='?', help='输入的JSONL文件路径')
    parser.add_argument('--market', choices=['HK', 'US'], help='指定市场类型(HK或US)')
    parser.add_argument('--batch', action='store_true', help='使用批量导入模式')
    parser.add_argument('--verify', action='store_true', help='验证导入结果')
    parser.add_argument('--export', help='导出当前数据库中的股票名称到指定文件')
    parser.add_argument('--create-example', help='创建示例JSONL文件')
    
    args = parser.parse_args()
    logger = setup_logging()
    
    # 创建示例文件
    if args.create_example:
        create_example_jsonl(args.create_example)
        return
    
    # 导出当前数据库中的股票名称
    if args.export:
        export_current_stock_names(args.export)
        return
    
    # 导入股票名称
    if args.input_file:
        records = read_jsonl_file(args.input_file)
        
        if not records:
            logger.error("没有有效的记录可导入")
            return
        
        if args.batch:
            batch_import_stock_names(records, args.market)
        else:
            import_stock_names(records, args.market)
        
        if args.verify:
            verify_import(records)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
