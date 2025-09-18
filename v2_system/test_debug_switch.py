#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
测试调试开关功能
验证非开市时间数据更新控制开关
"""

import sys
import os

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from config import (
    is_hk_trading_time,
    is_us_trading_time,
    should_monitor_market,
    should_update_data_off_hours,
    HK_TRADING_HOURS,
    US_TRADING_HOURS_DST,
    US_TRADING_HOURS_STD,
    is_us_dst
)

def test_debug_switch():
    """测试调试开关功能"""
    print("🔧 调试开关功能测试")
    print("=" * 50)
    
    # 检查当前交易状态
    hk_trading = is_hk_trading_time()
    us_trading = is_us_trading_time()
    
    print(f"📊 当前交易状态:")
    print(f"  🇭🇰 港股交易时间: {hk_trading}")
    print(f"  🇺🇸 美股交易时间: {us_trading}")
    print(f"  🌍 美国夏令时: {is_us_dst()}")
    
    print(f"\n🔧 当前调试开关配置:")
    hk_off_hours = should_update_data_off_hours('HK')
    us_off_hours = should_update_data_off_hours('US')
    print(f"  🇭🇰 港股非开市时间更新: {hk_off_hours}")
    print(f"  🇺🇸 美股非开市时间更新: {us_off_hours}")
    
    print(f"\n🎯 监控决策结果:")
    hk_should_monitor = should_monitor_market('HK')
    us_should_monitor = should_monitor_market('US')
    print(f"  🇭🇰 港股是否监控: {hk_should_monitor}")
    print(f"  🇺🇸 美股是否监控: {us_should_monitor}")
    
    print(f"\n📝 决策逻辑说明:")
    print(f"  港股: 交易时间({hk_trading}) OR 调试开关({hk_off_hours}) = {hk_should_monitor}")
    print(f"  美股: 交易时间({us_trading}) OR 调试开关({us_off_hours}) = {us_should_monitor}")
    
    print(f"\n⚙️  配置修改方法:")
    print(f"  港股: config.py -> HK_TRADING_HOURS['update_data_off_hours'] = True/False")
    print(f"  美股: config.py -> US_TRADING_HOURS_DST/STD['update_data_off_hours'] = True/False")
    
    # 测试不同配置场景
    print(f"\n🧪 测试场景:")
    
    scenarios = [
        ("交易时间 + 开关开启", True, True, True),
        ("交易时间 + 开关关闭", True, False, True),
        ("非交易时间 + 开关开启", False, True, True),
        ("非交易时间 + 开关关闭", False, False, False),
    ]
    
    for desc, is_trading, switch_on, expected in scenarios:
        result = is_trading or switch_on
        status = "✅" if result == expected else "❌"
        print(f"  {status} {desc}: {result}")

def main():
    """主函数"""
    test_debug_switch()
    
    print(f"\n💡 使用建议:")
    print(f"  - 生产环境: 建议关闭调试开关，只在交易时间监控")
    print(f"  - 调试环境: 开启调试开关，方便随时测试")
    print(f"  - 混合模式: 可以分别控制港股和美股的调试开关")

if __name__ == "__main__":
    main()
