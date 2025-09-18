#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
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
        filter_zero_diff = request.args.get('filter_zero_diff', 'false').lower() == 'true'
        
        # 查询数据
        trades_data = get_trades_data(market, page, per_page, stock_code, option_code, date_from, date_to, filter_zero_diff)
        
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
                                 'filter_zero_diff': filter_zero_diff
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
        filter_zero_diff = request.args.get('filter_zero_diff', 'false').lower() == 'true'
        
        trades_data = get_trades_data(market, page, per_page, stock_code, option_code, date_from, date_to, filter_zero_diff)
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

def get_trades_data(market='HK', page=1, per_page=50, stock_code='', option_code='', date_from='', date_to='', filter_zero_diff=False):
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
            
            # 🔥 新增：过滤变化量为0的期权
            if filter_zero_diff:
                where_conditions.append("ot.volume_diff != 0")
            
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
            
            # 查询当前期间和对比期间的数据，计算股票粒度的净持仓变化
            cursor.execute(f"""
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
            """, current_params + compare_params)
            
            stocks = []
            for row in cursor.fetchall():
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