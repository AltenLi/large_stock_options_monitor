#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
修正多市场数据库中的数据问题：
1. 修正变化量计算（与上一条记录比较）
2. 修正股票名称（从stock_info表补充）
3. 清理跨市场数据污染（US数据库删除HK数据，HK数据库删除US数据）
"""

import os
import sys
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple

# 添加V2系统路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from config import HK_DATABASE_CONFIG, US_DATABASE_CONFIG, get_stock_name
from utils.logger import setup_logger


class MultiMarketVolumeFixProcessor:
    """多市场变化量和数据清理修正处理器"""
    
    def __init__(self):
        self.logger = setup_logger('MultiMarketVolumeFixProcessor')
        # 获取两个市场的数据库路径
        self.hk_db_path = HK_DATABASE_CONFIG['db_path']
        self.us_db_path = US_DATABASE_CONFIG['db_path']
        
        self.logger.info(f"HK数据库路径: {self.hk_db_path}")
        self.logger.info(f"US数据库路径: {self.us_db_path}")
        
    def get_all_trades_by_date(self, db_path: str, market: str) -> Dict[str, List[Tuple]]:
        """获取指定数据库的所有交易记录，按日期分组"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT id, option_code, trade_date, volume, timestamp, volume_diff, last_volume, stock_code
                    FROM option_trades 
                    ORDER BY trade_date, option_code, timestamp
                """)
                
                all_records = cursor.fetchall()
                
                # 按日期分组
                trades_by_date = {}
                for record in all_records:
                    trade_date = record[2]  # trade_date
                    if trade_date not in trades_by_date:
                        trades_by_date[trade_date] = []
                    trades_by_date[trade_date].append(record)
                
                self.logger.info(f"{market}数据库共找到 {len(all_records)} 条交易记录")
                return trades_by_date
                
        except Exception as e:
            self.logger.error(f"获取{market}数据库交易记录失败: {e}")
            return {}
    
    def clean_cross_market_data(self, db_path: str, market: str) -> bool:
        """清理跨市场数据污染"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                if market == 'HK':
                    # 从HK数据库删除US数据（股票代码以US.开头的）
                    cursor.execute("SELECT COUNT(*) FROM option_trades WHERE stock_code LIKE 'US.%'")
                    us_count = cursor.fetchone()[0]
                    
                    if us_count > 0:
                        self.logger.info(f"在HK数据库中发现 {us_count} 条US数据，准备删除...")
                        cursor.execute("DELETE FROM option_trades WHERE stock_code LIKE 'US.%'")
                        deleted_count = cursor.rowcount
                        self.logger.info(f"从HK数据库删除了 {deleted_count} 条US数据")
                    else:
                        self.logger.info("HK数据库中没有发现US数据")
                        
                elif market == 'US':
                    # 从US数据库删除HK数据（股票代码以HK.开头的）
                    cursor.execute("SELECT COUNT(*) FROM option_trades WHERE stock_code LIKE 'HK.%'")
                    hk_count = cursor.fetchone()[0]
                    
                    if hk_count > 0:
                        self.logger.info(f"在US数据库中发现 {hk_count} 条HK数据，准备删除...")
                        cursor.execute("DELETE FROM option_trades WHERE stock_code LIKE 'HK.%'")
                        deleted_count = cursor.rowcount
                        self.logger.info(f"从US数据库删除了 {deleted_count} 条HK数据")
                    else:
                        self.logger.info("US数据库中没有发现HK数据")
                
                conn.commit()
                return True
                
        except Exception as e:
            self.logger.error(f"清理{market}数据库跨市场数据失败: {e}")
            return False
    
    def calculate_correct_volume_diff(self, trades_by_date: Dict[str, List[Tuple]], market: str) -> List[Tuple]:
        """计算正确的变化量"""
        updates = []
        
        for trade_date, records in trades_by_date.items():
            self.logger.info(f"处理{market}市场日期: {trade_date}, 记录数: {len(records)}")
            
            # 按期权代码分组
            trades_by_option = {}
            for record in records:
                option_code = record[1]
                if option_code not in trades_by_option:
                    trades_by_option[option_code] = []
                trades_by_option[option_code].append(record)
            
            # 为每个期权计算正确的变化量
            for option_code, option_records in trades_by_option.items():
                # 按时间戳排序
                option_records.sort(key=lambda x: x[4])  # timestamp
                
                for i, record in enumerate(option_records):
                    record_id = record[0]
                    current_volume = record[3]
                    
                    if i == 0:
                        # 第一条记录，变化量就是当前成交量
                        previous_volume = 0
                        volume_diff = current_volume
                    else:
                        # 后续记录，与上一条记录比较
                        previous_record = option_records[i-1]
                        previous_volume = previous_record[3]
                        volume_diff = current_volume - previous_volume
                    
                    # 只有当计算出的值与数据库中的值不同时才更新
                    db_volume_diff = record[5]
                    db_last_volume = record[6]
                    
                    if db_volume_diff != volume_diff or db_last_volume != previous_volume:
                        updates.append((volume_diff, previous_volume, record_id))
        
        return updates
    
    def update_database(self, db_path: str, market: str, updates: List[Tuple]) -> bool:
        """批量更新指定数据库"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                cursor.executemany("""
                    UPDATE option_trades 
                    SET volume_diff = ?, last_volume = ?
                    WHERE id = ?
                """, updates)
                
                conn.commit()
                self.logger.info(f"成功更新{market}数据库 {len(updates)} 条记录")
                return True
                
        except Exception as e:
            self.logger.error(f"更新{market}数据库失败: {e}")
            return False
    
    def fix_all_data(self) -> bool:
        """修正所有数据（跨市场数据清理、变化量和股票名称）"""
        try:
            self.logger.info("开始修正多市场数据库中的数据...")
            
            # 检查数据库文件是否存在
            markets_to_process = []
            if os.path.exists(self.hk_db_path):
                markets_to_process.append(('HK', self.hk_db_path))
            else:
                self.logger.warning(f"HK数据库文件不存在: {self.hk_db_path}")
                
            if os.path.exists(self.us_db_path):
                markets_to_process.append(('US', self.us_db_path))
            else:
                self.logger.warning(f"US数据库文件不存在: {self.us_db_path}")
            
            if not markets_to_process:
                self.logger.error("没有找到任何数据库文件")
                return False
            
            all_success = True
            
            for market, db_path in markets_to_process:
                self.logger.info("=" * 60)
                self.logger.info(f"处理 {market} 市场数据库: {db_path}")
                self.logger.info("=" * 60)
                
                # 1. 清理跨市场数据污染
                self.logger.info(f"第一步：清理{market}数据库中的跨市场数据")
                self.logger.info("-" * 40)
                
                clean_success = self.clean_cross_market_data(db_path, market)
                if clean_success:
                    self.logger.info(f"✅ {market}数据库跨市场数据清理完成！")
                else:
                    self.logger.error(f"❌ {market}数据库跨市场数据清理失败！")
                    all_success = False
                    continue
                
                # 2. 修正变化量
                self.logger.info(f"第二步：修正{market}数据库变化量")
                self.logger.info("-" * 40)
                
                # 获取交易记录
                trades_by_date = self.get_all_trades_by_date(db_path, market)
                if not trades_by_date:
                    self.logger.warning(f"{market}数据库没有找到交易记录")
                    continue
                
                total_records = sum(len(records) for records in trades_by_date.values())
                self.logger.info(f"{market}数据库共找到 {total_records} 条交易记录，涉及 {len(trades_by_date)} 个交易日")
                
                # 计算正确的变化量
                volume_updates = self.calculate_correct_volume_diff(trades_by_date, market)
                
                if volume_updates:
                    self.logger.info(f"{market}数据库需要更新 {len(volume_updates)} 条记录的变化量")
                    volume_success = self.update_database(db_path, market, volume_updates)
                    if volume_success:
                        self.logger.info(f"✅ {market}数据库变化量修正完成！")
                    else:
                        self.logger.error(f"❌ {market}数据库变化量修正失败！")
                        all_success = False
                else:
                    self.logger.info(f"✅ {market}数据库所有记录的变化量都是正确的，无需更新")
                
                # 3. 修正股票名称
                self.logger.info(f"第三步：修正{market}数据库股票名称")
                self.logger.info("-" * 40)
                
                name_success = self.fix_stock_names(db_path, market)
                if name_success:
                    self.logger.info(f"✅ {market}数据库股票名称修正完成！")
                else:
                    self.logger.error(f"❌ {market}数据库股票名称修正失败！")
                    all_success = False
            
            return all_success
            
        except Exception as e:
            self.logger.error(f"修正数据过程中出错: {e}")
            return False
    
    def get_stock_names_from_stock_info(self, db_path: str, market: str) -> Dict[str, str]:
        """从指定数据库的stock_info表获取股票名称映射"""
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT stock_code, stock_name 
                    FROM stock_info 
                    WHERE stock_name IS NOT NULL AND stock_name != ''
                """)
                
                results = cursor.fetchall()
                stock_names = {stock_code: stock_name for stock_code, stock_name in results}
                
                self.logger.info(f"从{market}数据库stock_info表获取到 {len(stock_names)} 个股票名称")
                return stock_names
                
        except Exception as e:
            self.logger.error(f"获取{market}数据库股票名称映射失败: {e}")
            return {}
    
    def fix_stock_names(self, db_path: str, market: str) -> bool:
        """修正指定数据库option_trades表中的股票名称"""
        try:
            self.logger.info(f"开始修正{market}数据库股票名称...")
            
            # 获取股票名称映射
            stock_names = self.get_stock_names_from_stock_info(db_path, market)
            if not stock_names:
                self.logger.warning(f"{market}数据库没有找到股票名称数据，将使用配置文件中的股票名称")
                # 使用配置文件中的股票名称
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT DISTINCT stock_code FROM option_trades WHERE stock_name IS NULL OR stock_name = '' OR stock_name = '-'")
                    stock_codes = [row[0] for row in cursor.fetchall()]
                    
                    for stock_code in stock_codes:
                        stock_name = get_stock_name(stock_code)
                        if stock_name != stock_code:  # 如果找到了名称（不等于代码本身）
                            stock_names[stock_code] = stock_name
                
                if not stock_names:
                    self.logger.warning(f"配置文件中也没有找到{market}市场的股票名称")
                    return False
                else:
                    self.logger.info(f"从配置文件中获取到 {len(stock_names)} 个股票名称")
            
            # 查找需要更新的记录
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # 找出股票名称为空或'-'的记录，且在stock_info中有对应名称的
                placeholders = ','.join(['?' for _ in stock_names.keys()])
                cursor.execute(f"""
                    SELECT DISTINCT stock_code 
                    FROM option_trades 
                    WHERE (stock_name IS NULL OR stock_name = '' OR stock_name = '-')
                    AND stock_code IN ({placeholders})
                """, list(stock_names.keys()))
                
                codes_to_update = [row[0] for row in cursor.fetchall()]
                
                if not codes_to_update:
                    self.logger.info(f"{market}数据库所有股票名称都已正确，无需更新")
                    return True
                
                self.logger.info(f"{market}数据库需要更新 {len(codes_to_update)} 个股票代码的名称")
                
                # 批量更新股票名称
                updates = []
                for stock_code in codes_to_update:
                    stock_name = stock_names[stock_code]
                    updates.append((stock_name, stock_code))
                
                cursor.executemany("""
                    UPDATE option_trades 
                    SET stock_name = ?
                    WHERE stock_code = ? AND (stock_name IS NULL OR stock_name = '' OR stock_name = '-')
                """, updates)
                
                updated_count = cursor.rowcount
                conn.commit()
                
                self.logger.info(f"成功更新{market}数据库 {updated_count} 条记录的股票名称")
                return True
                
        except Exception as e:
            self.logger.error(f"修正{market}数据库股票名称失败: {e}")
            return False
    
    def verify_fix(self) -> bool:
        """验证多市场数据库修正结果"""
        try:
            markets_to_verify = []
            if os.path.exists(self.hk_db_path):
                markets_to_verify.append(('HK', self.hk_db_path))
            if os.path.exists(self.us_db_path):
                markets_to_verify.append(('US', self.us_db_path))
            
            all_success = True
            
            for market, db_path in markets_to_verify:
                self.logger.info(f"验证{market}数据库修正结果...")
                
                with sqlite3.connect(db_path) as conn:
                    cursor = conn.cursor()
                    
                    # 检查跨市场数据污染
                    if market == 'HK':
                        cursor.execute("SELECT COUNT(*) FROM option_trades WHERE stock_code LIKE 'US.%'")
                        cross_market_count = cursor.fetchone()[0]
                        if cross_market_count > 0:
                            self.logger.warning(f"HK数据库仍有 {cross_market_count} 条US数据")
                            all_success = False
                        else:
                            self.logger.info("HK数据库已无US数据污染")
                    elif market == 'US':
                        cursor.execute("SELECT COUNT(*) FROM option_trades WHERE stock_code LIKE 'HK.%'")
                        cross_market_count = cursor.fetchone()[0]
                        if cross_market_count > 0:
                            self.logger.warning(f"US数据库仍有 {cross_market_count} 条HK数据")
                            all_success = False
                        else:
                            self.logger.info("US数据库已无HK数据污染")
                    
                    # 检查是否还有异常的变化量
                    cursor.execute("""
                        SELECT COUNT(*) FROM option_trades 
                        WHERE volume_diff < 0 OR volume_diff > volume
                    """)
                    
                    abnormal_volume_count = cursor.fetchone()[0]
                    
                    # 检查是否还有空的股票名称
                    cursor.execute("""
                        SELECT COUNT(*) FROM option_trades 
                        WHERE stock_name IS NULL OR stock_name = '' OR stock_name = '-'
                    """)
                    
                    empty_name_count = cursor.fetchone()[0]
                    
                    if abnormal_volume_count > 0:
                        self.logger.warning(f"{market}数据库仍有 {abnormal_volume_count} 条记录的变化量可能异常")
                        all_success = False
                    else:
                        self.logger.info(f"{market}数据库所有记录的变化量都正常")
                    
                    if empty_name_count > 0:
                        self.logger.warning(f"{market}数据库仍有 {empty_name_count} 条记录的股票名称为空")
                        all_success = False
                    else:
                        self.logger.info(f"{market}数据库所有记录的股票名称都已填充")
            
            return all_success
                    
        except Exception as e:
            self.logger.error(f"验证修正结果失败: {e}")
            return False


def main():
    """主函数"""
    print("=" * 70)
    print("多市场期权交易记录数据修正工具")
    print("=" * 70)
    print("功能：")
    print("1. 清理跨市场数据污染（US数据库删除HK数据，HK数据库删除US数据）")
    print("2. 修正变化量计算（与上一条记录比较）")
    print("3. 修正股票名称（从stock_info表补充）")
    print("=" * 70)
    
    processor = MultiMarketVolumeFixProcessor()
    
    # 询问用户确认
    confirm = input("是否要修正多市场数据库中所有期权交易记录的数据？(y/N): ")
    if confirm.lower() != 'y':
        print("操作已取消")
        return
    
    # 执行修正
    success = processor.fix_all_data()
    
    if success:
        print("\n" + "=" * 70)
        print("✅ 多市场数据修正成功！")
        print("=" * 70)
        
        # 验证结果
        print("正在验证修正结果...")
        if processor.verify_fix():
            print("✅ 验证通过，所有数据都正确")
        else:
            print("⚠️  验证发现异常，请检查日志")
    else:
        print("\n" + "=" * 70)
        print("❌ 多市场数据修正失败，请检查日志")
        print("=" * 70)


if __name__ == "__main__":
    main()
