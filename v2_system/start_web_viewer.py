#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
large_stock_options_monitor
Copyright (C) 2025 AltenLi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

V2系统数据库浏览器启动脚本
"""

import os
import sys
import subprocess
import webbrowser
import time
from threading import Timer

# 将项目根目录与 v2_system 目录加入 sys.path，兼容模块与脚本两种运行方式
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

# 将项目根目录与 v2_system 目录加入 sys.path，兼容模块与脚本两种运行方式
_current_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_current_dir)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
if _current_dir not in sys.path:
    sys.path.insert(0, _current_dir)

def check_dependencies():
    """检查依赖包"""
    try:
        import flask
        print("✓ Flask已安装")
        return True
    except ImportError:
        print("✗ Flask未安装，正在安装...")
        try:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'flask', 'pandas'])
            print("✓ 依赖包安装完成")
            return True
        except subprocess.CalledProcessError:
            print("✗ 依赖包安装失败，请手动安装: pip install flask pandas")
            return False

def open_browser():
    """延迟打开浏览器"""
    time.sleep(2)  # 等待服务器启动
    webbrowser.open('http://localhost:5001')

def main():
    """主函数"""
    print("=" * 50)
    print("V2期权监控系统 - 数据库浏览器")
    print("=" * 50)
    
    # 检查依赖
    if not check_dependencies():
        return
    
    # 检查实际使用的数据库（HK/US）
    try:
        from v2_system.web_viewer import hk_db_manager, us_db_manager
        hk_db = getattr(hk_db_manager, 'db_path', None)
        us_db = getattr(us_db_manager, 'db_path', None)

        print("数据库路径检测：")
        if hk_db:
            if os.path.exists(hk_db):
                print(f"✓ 港股数据库: {hk_db}")
            else:
                print(f"⚠️ 港股数据库不存在: {hk_db}")
        else:
            print("⚠️ 未获取到港股数据库路径")

        if us_db:
            if os.path.exists(us_db):
                print(f"✓ 美股数据库: {us_db}")
            else:
                print(f"⚠️ 美股数据库不存在: {us_db}")
        else:
            print("⚠️ 未获取到美股数据库路径")
    except Exception as e:
        print(f"数据库路径检测失败: {e}")
    
    # 启动Web服务器
    print("\n启动Web服务器...")
    print("访问地址: http://localhost:5001")
    print("按 Ctrl+C 停止服务器")
    print("-" * 50)
    
    # 延迟打开浏览器
    Timer(2.0, open_browser).start()
    
    # 启动Flask应用
    try:
        from v2_system.web_viewer import app
        app.run(debug=False, host='0.0.0.0', port=5001)
    except KeyboardInterrupt:
        print("\n服务器已停止")
    except Exception as e:
        print(f"启动失败: {e}")

if __name__ == '__main__':
    main()
