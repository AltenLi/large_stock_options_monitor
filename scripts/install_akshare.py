#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安装akshare依赖包
"""

import subprocess
import sys

def install_package(package_name):
    """安装Python包"""
    try:
        print(f"正在安装 {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"✓ {package_name} 安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {package_name} 安装失败: {e}")
        return False

def main():
    """主函数"""
    print("=" * 50)
    print("安装akshare股价更新脚本依赖")
    print("=" * 50)
    
    # 需要安装的包
    packages = [
        "akshare",
        "pandas",
    ]
    
    success_count = 0
    for package in packages:
        if install_package(package):
            success_count += 1
        print()
    
    print("=" * 50)
    print(f"安装完成: {success_count}/{len(packages)} 个包安装成功")
    
    if success_count == len(packages):
        print("✓ 所有依赖安装成功，可以运行akshare股价更新脚本了")
        print("\n使用方法:")
        print("python scripts/update_prices_akshare.py")
    else:
        print("✗ 部分依赖安装失败，请检查网络连接或手动安装")

if __name__ == '__main__':
    main()