#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
large_stock_options_monitor
Copyright (C) 2025 AltenLi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

V2系统多市场数据库浏览器 - Flask Web应用
用于查看和查询港股和美股期权交易数据
"""

import os
import sys
from flask import Flask, render_template, request, jsonify
from datetime import datetime, timedelta
import sqlite3
import json

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_database_config
from utils.database_manager import get_database_manager

app = Flask(__name__)
app.config['SECRET_KEY'] = 'v2_option_monitor_secret_key'

# 初始化数据库管理器
hk_db_manager = get_database_manager('HK')
us_db_manager = get_database_manager('US')

def get_db_manager(market='HK'):
    """根据市场获取数据库管理器"""
    return us_db_manager if market == 'US' else hk_db_manager

def get_market_open_time(market='HK'):
    """获取市场开盘时间，复用config中的配置"""
    from config import HK_TRADING_HOURS, US_TRADING_HOURS_DST, US_TRADING_HOURS_STD, is_us_dst
    
    if market == 'HK':
        return HK_TRADING_HOURS['market_open'] + ':00'
    elif market == 'US':
        if is_us_dst():
            return US_TRADING_HOURS_DST['market_open'] + ':00'
        else:
            return US_TRADING_HOURS_STD['market_open'] + ':00'
    return '09:30:00'

def get_trading_dates(market='HK'):
    """根据市场和当前时间获取统计日期和对比日期
    复用config中的交易时间判断逻辑
    返回: (current_date, compare_date, is_trading)
    """
    from config import is_market_trading_time, HK_TRADING_HOURS, US_TRADING_HOURS_DST, US_TRADING_HOURS_STD, is_us_dst
    
    now = datetime.now()
    is_trading = is_market_trading_time(market)
    
    if is_trading:
        # 开盘中：显示当日数据，对比上一交易日
        if market == 'US':
            # 美股跨日处理：根据夏令时/冬令时获取收盘时间
            if is_us_dst():
                market_close = US_TRADING_HOURS_DST['market_close']
            else:
                market_close = US_TRADING_HOURS_STD['market_close']
            
            # 如果当前时间在收盘时间前（次日凌晨），算作前一天的交易
            if now.time() <= datetime.strptime(market_close, '%H:%M').time():
                current_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
                compare_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
            else:
                current_date = now.strftime('%Y-%m-%d')
                compare_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            # 港股正常处理
            current_date = now.strftime('%Y-%m-%d')
            compare_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # 开盘前：显示上一交易日数据，对比上上交易日
        if market == 'US':
            # 美股：根据当前时间判断
            if is_us_dst():
                market_open = US_TRADING_HOURS_DST['market_open']
            else:
                market_open = US_TRADING_HOURS_STD['market_open']
            
            if now.time() <= datetime.strptime(market_open, '%H:%M').time():
                current_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
                compare_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
            else:
                current_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
                compare_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            # 港股：显示昨天的数据
            current_date = (now - timedelta(days=1)).strftime('%Y-%m-%d')
            compare_date = (now - timedelta(days=2)).strftime('%Y-%m-%d')
    
    return current_date, compare_date, is_trading

@app.route('/')
def index():
    """主页 - 显示数据概览"""
    try:
        # 获取港股和美股统计
        hk_stats = get_database_stats('HK')
        us_stats = get_database_stats('US')
        
        return render_template('index.html', 
                             hk_stats=hk_stats, 
                             us_stats=us_stats)
    except Exception as e:
        return f"错误: {str(e)}"

@app.route('/api/stats')
def api_stats():
    """API - 获取数据库统计信息"""
    try:
        market = request.args.get('market', 'HK')
        stats = get_database_stats(market)
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/trades')
@app.route('/trades/<market>')
def trades(market='HK'):
    """交易记录页面"""
    try:
        # 获取查询参数
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        stock_code = request.args.get('stock_code', '')
        option_code = request.args.get('option_code', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        min_volume_diff = request.args.get('min_volume_diff', '')
        
        # 查询数据
        trades_data = get_trades_data(market, page, per_page, stock_code, option_code, date_from, date_to, min_volume_diff)
        
        return render_template('trades.html', 
                             trades=trades_data['trades'],
                             pagination=trades_data['pagination'],
                             market=market,
                             market_name='港股' if market == 'HK' else '美股',
                             currency='港币' if market == 'HK' else '美元',
                             filters={
                                 'stock_code': stock_code,
                                 'option_code': option_code,
                                 'date_from': date_from,
                                 'date_to': date_to,
                                 'min_volume_diff': min_volume_diff
                             })
    except Exception as e:
        return f"错误: {str(e)}"

@app.route('/api/trades')
@app.route('/api/trades/<market>')
def api_trades(market='HK'):
    """API - 获取交易记录"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        stock_code = request.args.get('stock_code', '')
        option_code = request.args.get('option_code', '')
        date_from = request.args.get('date_from', '')
        date_to = request.args.get('date_to', '')
        min_volume_diff = request.args.get('min_volume_diff', '')
        
        trades_data = get_trades_data(market, page, per_page, stock_code, option_code, date_from, date_to, min_volume_diff)
        return jsonify(trades_data)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/stocks')
@app.route('/stocks/<market>')
def stocks(market='HK'):
    """股票统计页面"""
    try:
        stock_stats = get_stock_stats(market)
        return render_template('stocks.html', 
                             stocks=stock_stats,
                             market=market,
                             market_name='港股' if market == 'HK' else '美股',
                             currency='港币' if market == 'HK' else '美元')
    except Exception as e:
        return f"错误: {str(e)}"

@app.route('/api/stocks')
@app.route('/api/stocks/<market>')
def api_stocks(market='HK'):
    """API - 获取股票统计"""
    try:
        stock_stats = get_stock_stats(market)
        return jsonify(stock_stats)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/options_comparison')
@app.route('/options_comparison/<market>')
def options_comparison(market='HK'):
    """期权Call和Put对比页面"""
    try:
        comparison_data = get_options_comparison_data(market)
        return render_template('options_comparison.html', 
                             stocks=comparison_data,
                             market=market,
                             market_name='港股' if market == 'HK' else '美股',
                             currency='港币' if market == 'HK' else '美元')
    except Exception as e:
        return f"错误: {str(e)}"

@app.route('/api/options_comparison')
@app.route('/api/options_comparison/<market>')
def api_options_comparison(market='HK'):
    """API - 获取期权Call和Put对比数据"""
    try:
        comparison_data = get_options_comparison_data(market)
        return jsonify(comparison_data)
    except Exception as e:
        return jsonify({'error': str(e)})

# 美股专用路由
@app.route('/us_stocks')
def us_stocks():
    """美股统计页面"""
    return stocks('US')

@app.route('/us_trades')
def us_trades():
    """美股交易记录页面"""
    return trades('US')

def get_database_stats(market='HK'):
    """获取数据库统计信息"""
    try:
        db_manager = get_db_manager(market)
        with sqlite3.connect(db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            # 总记录数
            cursor.execute("SELECT COUNT(*) FROM option_trades")
            total_trades = cursor.fetchone()[0]
            
            # 今日记录数
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("SELECT COUNT(*) FROM option_trades WHERE DATE(timestamp) = ?", (today,))
            today_trades = cursor.fetchone()[0]
            
            # 最早和最新记录时间
            cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM option_trades")
            min_time, max_time = cursor.fetchone()
            
            # 股票数量
            cursor.execute("SELECT COUNT(DISTINCT stock_code) FROM option_trades")
            stock_count = cursor.fetchone()[0]
            
            # 期权代码数量
            cursor.execute("SELECT COUNT(DISTINCT option_code) FROM option_trades")
            option_count = cursor.fetchone()[0]
            
            # 总成交金额
            cursor.execute("SELECT SUM(turnover) FROM option_trades")
            total_turnover = cursor.fetchone()[0] or 0
            
            return {
                'market': market,
                'market_name': '港股' if market == 'HK' else '美股',
                'currency': '港币' if market == 'HK' else '美元',
                'total_trades': total_trades,
                'today_trades': today_trades,
                'stock_count': stock_count,
                'option_count': option_count,
                'total_turnover': total_turnover,
                'earliest_record': min_time,
                'latest_record': max_time,
                'database_path': db_manager.db_path
            }
    except Exception as e:
        print(f"获取{market}市场统计信息失败: {e}")
        return {
            'market': market,
            'market_name': '港股' if market == 'HK' else '美股',
            'currency': '港币' if market == 'HK' else '美元',
            'total_trades': 0,
            'today_trades': 0,
            'stock_count': 0,
            'option_count': 0,
            'total_turnover': 0,
            'earliest_record': None,
            'latest_record': None,
            'database_path': ''
        }

def get_trades_data(market='HK', page=1, per_page=50, stock_code='', option_code='', date_from='', date_to='', min_volume_diff=''):
    """获取交易记录数据"""
    try:
        db_manager = get_db_manager(market)
        with sqlite3.connect(db_manager.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 构建查询条件
            where_conditions = []
            params = []
            
            if stock_code:
                where_conditions.append("ot.stock_code LIKE ?")
                params.append(f"%{stock_code}%")
            
            if option_code:
                where_conditions.append("ot.option_code LIKE ?")
                params.append(f"%{option_code}%")
            
            if date_from:
                where_conditions.append("DATE(ot.timestamp) >= ?")
                params.append(date_from)
            
            if date_to:
                where_conditions.append("DATE(ot.timestamp) <= ?")
                params.append(date_to)
            
            # 🔥 新增：过滤变化量小于指定值的期权
            if min_volume_diff and min_volume_diff.strip():
                try:
                    min_diff_value = int(min_volume_diff.strip())
                    where_conditions.append("ABS(ot.volume_diff) >= ?")
                    params.append(min_diff_value)
                except ValueError:
                    pass  # 忽略无效的数字输入
            
            where_clause = ""
            if where_conditions:
                where_clause = "WHERE " + " AND ".join(where_conditions)
            
            # 获取总数
            count_query = f"""
                SELECT COUNT(*) FROM option_trades ot
                LEFT JOIN stock_info si ON ot.stock_code = si.stock_code
                {where_clause}
            """
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
            
            # 计算分页
            offset = (page - 1) * per_page
            total_pages = (total_count + per_page - 1) // per_page
            
            # 获取数据
            data_query = f"""
                SELECT ot.*, 
                       COALESCE(si.stock_name, ot.stock_name, '') as stock_name,
                       ot.option_open_interest,
                       ot.option_net_open_interest,
                       ot.open_interest_diff as option_open_interest_diff,
                       ot.net_open_interest_diff as option_net_open_interest_diff
                FROM option_trades ot
                LEFT JOIN stock_info si ON ot.stock_code = si.stock_code
                {where_clause}
                ORDER BY ot.timestamp DESC
                LIMIT ? OFFSET ?
            """
            cursor.execute(data_query, params + [per_page, offset])
            
            trades = []
            for row in cursor.fetchall():
                trade = dict(row)
                # 格式化时间
                if trade['timestamp']:
                    trade['formatted_time'] = datetime.fromisoformat(trade['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                trades.append(trade)
            
            return {
                'trades': trades,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total_count,
                    'pages': total_pages,
                    'has_prev': page > 1,
                    'has_next': page < total_pages,
                    'prev_num': page - 1 if page > 1 else None,
                    'next_num': page + 1 if page < total_pages else None
                }
            }
    except Exception as e:
        print(f"获取{market}市场交易数据失败: {e}")
        return {'trades': [], 'pagination': {}}

def get_options_comparison_data(market='HK'):
    """获取期权Call和Put对比数据
    每个股票显示Call和Put的总成交额、持仓和净持仓
    只使用最近一次开盘后的数据，每个期权代码只取最新信息
    """
    try:
        db_manager = get_db_manager(market)
        with sqlite3.connect(db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            # 根据市场和当前时间确定统计日期
            current_date, _, is_trading = get_trading_dates(market)
            
            # 根据是否在交易时间调整查询条件
            if is_trading:
                # 开盘后：查询当日开盘至今的数据
                time_condition = "DATE(ot.timestamp) = ? AND TIME(ot.timestamp) >= ?"
                market_open_time = get_market_open_time(market)
                query_params = [current_date, market_open_time]
            else:
                # 开盘前：查询完整交易日数据
                time_condition = "DATE(ot.timestamp) = ?"
                query_params = [current_date]
            
            print(f"[DEBUG] 期权对比查询 - 市场: {market}, 日期: {current_date}, 交易中: {is_trading}")
            print(f"[DEBUG] 查询参数: {query_params}")
            
            # 查询每个股票的Call和Put期权数据
            query_sql = f"""
                WITH latest_records AS (
                    SELECT 
                        ot.stock_code,
                        ot.option_code,
                        ot.option_type,
                        ot.volume,
                        ot.turnover,
                        ot.price,
                        ot.timestamp,
                        ot.option_open_interest,
                        ot.option_net_open_interest,
                        ROW_NUMBER() OVER (
                            PARTITION BY ot.option_code 
                            ORDER BY ot.timestamp DESC
                        ) as rn
                    FROM option_trades ot
                    WHERE {time_condition}
                ),
                stock_summary AS (
                    SELECT 
                        lr.stock_code,
                        lr.option_type,
                        COUNT(*) as trade_count,
                        SUM(lr.volume) as total_volume,
                        SUM(lr.turnover) as total_turnover,
                        AVG(lr.price) as avg_price,
                        MAX(lr.timestamp) as latest_trade,
                        SUM(COALESCE(lr.option_open_interest, 0)) as total_open_interest,
                        SUM(COALESCE(lr.option_net_open_interest, 0)) as total_net_open_interest
                    FROM latest_records lr
                    WHERE lr.rn = 1
                    GROUP BY lr.stock_code, lr.option_type
                )
                SELECT 
                    ss.stock_code,
                    COALESCE(si.stock_name, '') as stock_name,
                    ss.option_type,
                    ss.trade_count,
                    ss.total_volume,
                    ss.total_turnover,
                    ss.avg_price,
                    ss.latest_trade,
                    ss.total_open_interest,
                    ss.total_net_open_interest
                FROM stock_summary ss
                LEFT JOIN stock_info si ON ss.stock_code = si.stock_code
                ORDER BY ss.stock_code, ss.option_type
            """
            
            print(f"[DEBUG] 执行SQL查询...")
            cursor.execute(query_sql, query_params)
            
            # 处理数据，按股票分组，每个股票包含Call和Put数据
            raw_data = cursor.fetchall()
            print(f"[DEBUG] 查询结果总数: {len(raw_data)}")
            
            # 打印所有查询到的数据
            for i, row in enumerate(raw_data):
                print(f"[DEBUG] 记录 {i+1}: 股票代码={row[0]}, 股票名称={row[1]}, 期权类型={row[2]}, 交易笔数={row[3]}, 成交额={row[5]}")
            
            # 特别检查800000恒生指数的数据
            hsi_data = [row for row in raw_data if row[0] == '800000']
            if hsi_data:
                print(f"[DEBUG] 找到恒生指数(800000)数据 {len(hsi_data)} 条:")
                for row in hsi_data:
                    print(f"[DEBUG] HSI: 期权类型={row[2]}, 交易笔数={row[3]}, 成交额={row[5]}, 持仓={row[8]}, 净持仓={row[9]}")
            else:
                print(f"[DEBUG] 未找到恒生指数(800000)的数据")
                
                # 进一步检查数据库中是否有800000的记录
                cursor.execute("SELECT COUNT(*) FROM option_trades WHERE stock_code = '800000'")
                total_800000 = cursor.fetchone()[0]
                print(f"[DEBUG] 数据库中800000总记录数: {total_800000}")
                
                if total_800000 > 0:
                    cursor.execute(f"SELECT COUNT(*) FROM option_trades WHERE stock_code = '800000' AND {time_condition}", query_params)
                    filtered_800000 = cursor.fetchone()[0]
                    print(f"[DEBUG] 符合时间条件的800000记录数: {filtered_800000}")
                    
                    # 查看最新的几条800000记录
                    cursor.execute("SELECT stock_code, option_code, option_type, timestamp, volume, turnover FROM option_trades WHERE stock_code = '800000' ORDER BY timestamp DESC LIMIT 5")
                    recent_800000 = cursor.fetchall()
                    print(f"[DEBUG] 800000最新5条记录:")
                    for record in recent_800000:
                        print(f"[DEBUG]   {record}")
            
            stocks_dict = {}
            
            for row in raw_data:
                stock_code = row[0]
                stock_name = row[1]
                option_type = row[2]
                
                if stock_code not in stocks_dict:
                    stocks_dict[stock_code] = {
                        'stock_code': stock_code,
                        'stock_name': stock_name,
                        'call_data': {
                            'trade_count': 0,
                            'total_volume': 0,
                            'total_turnover': 0,
                            'avg_price': 0,
                            'total_open_interest': 0,
                            'total_net_open_interest': 0,
                            'latest_trade': None
                        },
                        'put_data': {
                            'trade_count': 0,
                            'total_volume': 0,
                            'total_turnover': 0,
                            'avg_price': 0,
                            'total_open_interest': 0,
                            'total_net_open_interest': 0,
                            'latest_trade': None
                        }
                    }
                
                # 根据期权类型填充数据
                data_key = 'call_data' if option_type == 'Call' else 'put_data'
                stocks_dict[stock_code][data_key] = {
                    'trade_count': row[3],
                    'total_volume': row[4] or 0,
                    'total_turnover': row[5] or 0,
                    'avg_price': round(row[6], 3) if row[6] else 0,
                    'total_open_interest': row[8] or 0,
                    'total_net_open_interest': row[9] or 0,
                    'latest_trade': row[7]
                }
                
                # 格式化最新交易时间
                if row[7]:
                    try:
                        formatted_time = datetime.fromisoformat(str(row[7])).strftime('%Y-%m-%d %H:%M:%S')
                        stocks_dict[stock_code][data_key]['formatted_latest'] = formatted_time
                    except:
                        stocks_dict[stock_code][data_key]['formatted_latest'] = str(row[7])
                else:
                    stocks_dict[stock_code][data_key]['formatted_latest'] = ''
            
            # 转换为列表并按总成交额排序
            stocks_list = list(stocks_dict.values())
            stocks_list.sort(key=lambda x: (x['call_data']['total_turnover'] + x['put_data']['total_turnover']), reverse=True)
            
            return stocks_list
    except Exception as e:
        print(f"获取{market}市场期权对比数据失败: {e}")
        return []

def get_stock_stats(market='HK'):
    """获取股票统计信息，按Put和Call分别统计
    根据当前时间和市场开盘状态决定统计逻辑：
    - 开盘前：显示上一交易日数据，对比上上交易日
    - 开盘后：显示当日开盘至今数据，对比上一交易日
    """
    try:
        db_manager = get_db_manager(market)
        with sqlite3.connect(db_manager.db_path) as conn:
            cursor = conn.cursor()
            
            # 根据市场和当前时间确定统计日期和对比日期
            current_date, compare_date, is_trading = get_trading_dates(market)
            
            # 根据是否在交易时间调整查询条件
            if is_trading:
                # 开盘后：查询当日开盘至今的数据
                time_condition_current = "DATE(ot.timestamp) = ? AND TIME(ot.timestamp) >= ?"
                time_condition_compare = "DATE(ot.timestamp) = ?"
                market_open_time = get_market_open_time(market)
                current_params = [current_date, market_open_time]
                compare_params = [compare_date]
            else:
                # 开盘前：查询完整交易日数据
                time_condition_current = "DATE(ot.timestamp) = ?"
                time_condition_compare = "DATE(ot.timestamp) = ?"
                current_params = [current_date]
                compare_params = [compare_date]
            
            print(f"[DEBUG] 股票统计查询 - 市场: {market}, 当前日期: {current_date}, 对比日期: {compare_date}, 交易中: {is_trading}")
            print(f"[DEBUG] 当前查询参数: {current_params}, 对比查询参数: {compare_params}")
            
            # 查询当前期间和对比期间的数据，计算股票粒度的净持仓变化
            query_sql = f"""
                WITH current_latest AS (
                    SELECT 
                        ot.stock_code,
                        ot.option_code,
                        ot.option_type,
                        ot.volume,
                        ot.turnover,
                        ot.price,
                        ot.timestamp,
                        ot.option_open_interest,
                        ot.option_net_open_interest,
                        ROW_NUMBER() OVER (
                            PARTITION BY ot.option_code 
                            ORDER BY ot.timestamp DESC
                        ) as rn
                    FROM option_trades ot
                    WHERE {time_condition_current}
                ),
                compare_latest AS (
                    SELECT 
                        ot.stock_code,
                        ot.option_code,
                        ot.option_type,
                        ot.option_open_interest,
                        ot.option_net_open_interest,
                        ROW_NUMBER() OVER (
                            PARTITION BY ot.option_code 
                            ORDER BY ot.timestamp DESC
                        ) as rn
                    FROM option_trades ot
                    WHERE {time_condition_compare}
                ),
                current_summary AS (
                    SELECT 
                        cl.stock_code,
                        cl.option_type,
                        COUNT(*) as trade_count,
                        SUM(cl.volume) as total_volume,
                        SUM(cl.turnover) as total_turnover,
                        AVG(cl.price) as avg_price,
                        MAX(cl.timestamp) as latest_trade,
                        SUM(COALESCE(cl.option_open_interest, 0)) as total_open_interest,
                        SUM(COALESCE(cl.option_net_open_interest, 0)) as current_total_net_open_interest
                    FROM current_latest cl
                    WHERE cl.rn = 1
                    GROUP BY cl.stock_code, cl.option_type
                ),
                compare_summary AS (
                    SELECT 
                        cl.stock_code,
                        cl.option_type,
                        SUM(COALESCE(cl.option_open_interest, 0)) as compare_total_open_interest,
                        SUM(COALESCE(cl.option_net_open_interest, 0)) as compare_total_net_open_interest
                    FROM compare_latest cl
                    WHERE cl.rn = 1
                    GROUP BY cl.stock_code, cl.option_type
                )
                SELECT 
                    cs.stock_code,
                    COALESCE(si.stock_name, '') as stock_name,
                    COALESCE(cs.option_type, 'Unknown') as option_type,
                    cs.trade_count,
                    cs.total_volume,
                    cs.total_turnover,
                    cs.avg_price,
                    cs.latest_trade,
                    cs.total_open_interest,
                    cs.current_total_net_open_interest,
                    COALESCE(cms.compare_total_open_interest, 0) as compare_total_open_interest,
                    COALESCE(cms.compare_total_net_open_interest, 0) as compare_total_net_open_interest,
                    (cs.total_open_interest - COALESCE(cms.compare_total_open_interest, 0)) as open_interest_change,
                    (cs.current_total_net_open_interest - COALESCE(cms.compare_total_net_open_interest, 0)) as net_open_interest_change
                FROM current_summary cs
                LEFT JOIN compare_summary cms ON cs.stock_code = cms.stock_code AND cs.option_type = cms.option_type
                LEFT JOIN stock_info si ON cs.stock_code = si.stock_code
                ORDER BY cs.total_turnover DESC
            """
            
            print(f"[DEBUG] 执行股票统计SQL查询...")
            cursor.execute(query_sql, current_params + compare_params)
            
            result_data = cursor.fetchall()
            print(f"[DEBUG] 股票统计查询结果总数: {len(result_data)}")
            
            # 特别检查800000恒生指数的数据
            hsi_stats = [row for row in result_data if row[0] == '800000']
            if hsi_stats:
                print(f"[DEBUG] 找到恒生指数(800000)统计数据 {len(hsi_stats)} 条:")
                for row in hsi_stats:
                    print(f"[DEBUG] HSI统计: 期权类型={row[2]}, 交易笔数={row[3]}, 成交额={row[5]}, 持仓={row[8]}, 净持仓={row[9]}")
            else:
                print(f"[DEBUG] 未找到恒生指数(800000)的统计数据")
            
            stocks = []
            for row in result_data:
                stock = {
                    'stock_code': row[0],
                    'stock_name': row[1],
                    'option_type': row[2],
                    'trade_count': row[3],
                    'total_volume': row[4] or 0,
                    'total_turnover': row[5] or 0,
                    'avg_price': round(row[6], 3) if row[6] else 0,
                    'latest_trade': row[7],
                    'total_open_interest': row[8] or 0,
                    'total_net_open_interest': row[9] or 0,
                    'compare_total_open_interest': row[10] or 0,
                    'compare_total_net_open_interest': row[11] or 0,
                    'total_open_interest_diff': row[12] or 0,  # 持仓变化量
                    'total_net_open_interest_diff': row[13] or 0  # 净持仓变化量
                }
                if stock['latest_trade']:
                    try:
                        stock['formatted_latest'] = datetime.fromisoformat(str(stock['latest_trade'])).strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        stock['formatted_latest'] = str(stock['latest_trade'])
                else:
                    stock['formatted_latest'] = ''
                stocks.append(stock)
            
            return stocks
    except Exception as e:
        print(f"获取{market}市场股票统计失败: {e}")
        return []

if __name__ == '__main__':
    # 确保模板目录存在
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    os.makedirs(template_dir, exist_ok=True)
    
    print("V2多市场期权监控数据库浏览器启动中...")
    print(f"港股数据库: {hk_db_manager.db_path}")
    print(f"美股数据库: {us_db_manager.db_path}")
    print("访问地址: http://localhost:5001")
    
    app.run(debug=True, host='0.0.0.0', port=5001)
