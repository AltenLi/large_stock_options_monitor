# 🚀 V2 多市场期权大单监控系统

[![License](https://img.shields.io/badge/License-GPL%203.0%20or%20later-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html)
[![Python](https://img.shields.io/badge/Python-3.11%2B-green.svg)](https://www.python.org/)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen.svg)]()
[![Markets](https://img.shields.io/badge/Markets-HK%20%7C%20US-orange.svg)]()

> 🎯 **专业级多市场期权大单实时监控系统** - 基于富途 API 的高性能期权交易监控解决方案，支持港股和美股期权监控

## ✨ 核心特性

### 🔥 **多市场实时监控**
- 🇭🇰 **港股期权** - 实时监控港股期权大单交易
- 🇺🇸 **美股期权** - 支持美股期权大单监控
- 🚨 **毫秒级响应** - 实时捕获期权大单交易
- 📊 **智能筛选** - 可配置的成交量、成交额阈值
- 🎯 **精准识别** - 自动识别 Call/Put 期权类型
- ⚡ **高效处理** - 支持数百只期权同时监控
- 🌍 **跨时区支持** - 智能识别不同市场交易时间

### 📈 **数据分析**
- 📋 **分类统计** - 按股票代码和期权类型（Put/Call）分别统计
- 🔍 **变化量追踪** - 精确计算与上一条记录的成交量差值
- 📊 **多维度排行** - 成交额、交易笔数、成交量等多重排序
- 📅 **历史数据** - 完整的交易历史记录和趋势分析

### 🔔 **智能通知**
- 📱 **多渠道推送** - 企业微信、Mac 系统通知、控制台输出
- 📊 **汇总报告** - 统一发送所有期权变化的综合报告
- ⏰ **防骚扰机制** - 智能通知间隔控制，避免频繁推送
- 🎨 **美观格式** - 结构化的通知内容，易于阅读

### 🌐 **Web 界面**
- 💻 **现代化 UI** - 基于 Bootstrap 5 的响应式设计
- 🌍 **多市场支持** - 统一界面管理港股和美股数据
  - 🏠 **多市场首页** - 同时显示港股和美股统计概览
  - 🇭🇰 **港股专页** - `/stocks/HK` 港股期权数据和统计
  - 🇺🇸 **美股专页** - `/stocks/US` 美股期权数据和统计
  - 🔄 **市场切换** - 便捷的导航栏市场切换功能
- 💱 **货币显示** - 智能识别并显示对应货币单位（港币/美元）
- 📊 **实时数据** - 股票统计、交易记录实时展示
- 🔍 **高级筛选** - 支持股票代码、期权代码、日期范围筛选
- 📄 **分页浏览** - 高效的大数据量分页显示

### 🗄️ **数据管理**
- 💾 **分市场数据库** - 港股和美股数据完全分离存储
  - 🇭🇰 港股数据库: `data/hk_options_monitor_v2.db`
  - 🇺🇸 美股数据库: `data/us_options_monitor_v2.db`
- 🔄 **单例模式优化** - 每个市场使用独立的数据库管理器实例
- 🚀 **性能优化** - 避免数据库重复初始化，提升系统效率
- 🛠️ **数据修复** - 内置数据修正工具，确保数据准确性
- 📦 **自动备份** - 可配置的数据备份机制

## 🏗️ 系统架构

```
V2 多市场期权监控系统
├── 🎯 核心监控模块
│   ├── option_monitor_v2.py          # 通用监控引擎
│   ├── start_us_monitor.py           # 美股专用启动器
│   ├── start_hk_monitor.py           # 美股专用启动器
│   └── start_multi_market_monitor.py # 多市场并行监控
├── 📊 数据处理层 (utils/)
│   ├── database_manager.py           # 分市场数据库管理
│   │   ├── 🇭🇰 港股数据库管理器 (单例模式)
│   │   └── 🇺🇸 美股数据库管理器 (单例模式)
│   ├── big_options_processor.py      # 多市场大单处理器
│   ├── notifier.py                   # 智能通知系统
│   └── data_handler.py               # 市场感知数据处理
├── 🌐 Web 服务 (web_viewer.py)
│   ├── Flask 多市场 Web 应用
│   ├── 分市场 RESTful API 接口
│   ├── 🇭🇰 港股数据接口 (/stocks, /trades)
│   ├── 🇺🇸 美股数据接口 (/stocks, /trades)
│   └── 响应式多市场前端界面
├── 💾 分离式数据存储
│   ├── 🇭🇰 data/hk_options_monitor_v2.db
│   └── 🇺🇸 data/us_options_monitor_v2.db
└── 🛠️ 工具集
    ├── 数据修正工具 (fix_volume_diff.py)
    ├── 多市场配置管理 (config.py)
    ├── 数据库测试工具 (test_multi_db.py)
    └── 日志系统 (logger.py)
```

## 🚀 快速开始

### 📋 环境要求

- **Python**: 3.11+
- **操作系统**: Windows / macOS / Linux
- **内存**: 建议 2GB+
- **存储**: 建议 1GB+ 可用空间

### 📦 安装依赖

```bash
# 克隆项目
git clone <repository-url>
cd large_stock_options_monitor/v2_system

# 初始化配置文件
cp config.py.sample config.py

# 安装 Python 依赖
pip install -r requirements.txt

# 或手动安装核心依赖
pip install futu-api pandas numpy flask requests
```

### ⚙️ 配置设置

1. **编辑配置文件** `config.py`:

```python
# 富途 API 配置
FUTU_CONFIG = {
    'host': '127.0.0.1',    # 富途牛牛客户端地址
    'port': 11111,          # API 端口
    'market': 'HK'          # 市场代码
}

# 大单监控阈值
BIG_TRADE_CONFIG = {
    'min_volume_threshold': 100,      # 最小成交量(张)
    'min_turnover_threshold': 500000, # 最小成交额(港币)
    'notification_cooldown': 300      # 通知冷却时间(秒)
}

# 通知配置
NOTIFICATION = {
    'enable_wework_bot': True,        # 企业微信通知
    'enable_mac_notification': True,  # Mac 系统通知
    'wework_webhook': 'YOUR_WEBHOOK_URL'
}
```

2. **启动futu opend** 

### 🎯 运行系统

#### 🇭🇰 **港股期权监控**
```bash
# 启动港股期权监控程序
python start_hk_monitor.py

# 启动 Web 界面 (新终端)
python web_viewer.py
```

#### 🇺🇸 **美股期权监控**
```bash
# 启动美股期权监控程序
python start_us_monitor.py

# 启动 Web 界面 (新终端)
python web_viewer.py
```

#### 🌍 **多市场同时监控**
```bash
# 同时监控港股和美股期权 (推荐)
python start_multi_market_monitor.py

# 启动 Web 界面 (新终端)
python web_viewer.py
```

**🎯 多市场监控特性**:
- ✅ **独立数据库**: 港股和美股数据完全分离存储
- ✅ **并行监控**: 两个市场同时运行，互不干扰
- ✅ **智能调度**: 根据交易时间自动启停监控线程
- ✅ **资源优化**: 单例模式避免数据库重复初始化
- ✅ **实时切换**: Web界面支持港股/美股数据实时切换

访问 Web 界面: http://localhost:5001
- 🏠 **多市场首页**: `/` - 港股和美股统计概览
- 🇭🇰 **港股数据**: `/stocks/HK`  `/trades/HK` - 港股期权统计和交易记录
- 🇺🇸 **美股数据**: `/stocks/US`  `/trades/US` - 美股期权统计和交易记录

## 📊 功能详解

### 🔍 **多市场实时监控**

系统支持港股和美股期权的并行监控，当检测到符合条件的大单交易时：

1. **多市场数据采集**: 
   - 🇭🇰 港股: 使用富途 API 获取港股期权实时数据
   - 🇺🇸 美股: 使用富途 API 获取美股期权实时数据
2. **智能筛选**: 根据成交量、成交额阈值过滤
3. **变化计算**: 精确计算与上一条记录的差值
4. **分离式存储**: 
   - 🇭🇰 港股数据 → `hk_options_monitor_v2.db`
   - 🇺🇸 美股数据 → `us_options_monitor_v2.db`
5. **实时通知**: 发送到配置的通知渠道

### 🏗️ **数据库架构优化**

**🔧 单例模式设计**:
```python
# 每个市场独立的数据库管理器
hk_db_manager = get_hk_database_manager()  # 港股数据库单例
us_db_manager = get_us_database_manager()  # 美股数据库单例
```

**✅ 优化效果**:
- 🚀 **性能提升**: 避免重复数据库连接初始化
- 💾 **内存优化**: 每个市场只维护一个数据库连接
- 🔒 **线程安全**: 支持多线程并发访问
- 📊 **数据隔离**: 港股和美股数据完全分离

### 📈 **统计分析**

- **按股票统计**: 每个股票的 Put/Call 期权分别统计
- **排行榜**: 成交额、交易笔数、成交量等多维度排序
- **历史趋势**: 完整的交易历史和变化趋势
- **实时更新**: 数据实时刷新，确保信息准确性

### 🔔 **通知系统**

**汇总报告格式**:
```
📊 期权监控汇总报告
⏰ 时间: 2025-09-12 16:00:00
📈 总交易: 50 笔 (新增: 15 笔，符合通知条件: 8 笔)
💰 总金额: 5,000,000 港币 (新增: 2,000,000 港币)

📋 新增大单统计:
• 腾讯控股 (HK.00700): 5笔, 1,500,000港币 (股价: 320.50)
  1. HK.TCH250930C330000: Call, 2.500×1000张, +500张, 125.0万
  2. HK.TCH250930P310000: Put, 1.800×800张, +300张, 72.0万
```

## 🛠️ 高级功能

### 📊 **数据修正工具**

```bash
# 修正历史数据的变化量计算和股票名称
python fix_volume_diff.py
```

功能包括:
- ✅ 重新计算所有记录的变化量（与上一条记录比较）
- ✅ 从 stock_info 表补充缺失的股票名称
- ✅ 批量处理，安全高效
- ✅ 自动验证修正结果

### 🧪 **多市场数据库测试**

```bash
# 测试多市场数据库功能
python test_multi_db.py
```

测试功能:
- ✅ **数据库分离验证**: 确认港股和美股数据库独立存储
- ✅ **单例模式测试**: 验证每个市场只有一个数据库管理器实例
- ✅ **数据统计对比**: 显示各市场的数据量和最新记录
- ✅ **连接性能测试**: 验证数据库连接效率和稳定性

**测试输出示例**:
```
🧪 多市场数据库测试
==================

📊 数据库状态:
🇭🇰 港股数据库: data/hk_options_monitor_v2.db (479 条记录)
🇺🇸 美股数据库: data/us_options_monitor_v2.db (0 条记录)

✅ 单例模式验证:
🇭🇰 港股数据库管理器: 单例模式正常工作
🇺🇸 美股数据库管理器: 单例模式正常工作

✅ 数据库分离: 港股和美股数据完全分离存储
```

### 🔧 **配置优化**

**监控股票配置**:
```python
MONITORED_STOCKS = [
    'HK.00700',  # 腾讯控股
    'HK.09988',  # 阿里巴巴
    'HK.03690',  # 美团
    # ... 更多股票
]
```

**期权过滤配置**:
```python
OPTION_FILTERS = {
    'default': {
        'min_volume': 10,
        'min_turnover': 200000,
        'min_days_to_expiry': 1,
        'max_days_to_expiry': 365
    }
}
```

## 📁 项目结构

```
v2_system/
├── 📄 README.md                    # 项目文档
├── ⚙️ config.py                    # 配置文件
├── 🎯 option_monitor_v2.py         # 主监控程序
├── 🌐 web_viewer.py               # Web 界面
├── 🛠️ fix_volume_diff.py          # 数据修正工具
├── 📊 utils/                      # 工具模块
│   ├── 🗄️ database_manager.py     # 数据库管理
│   ├── 📈 big_options_processor.py # 大单处理器
│   ├── 🔔 notifier.py             # 通知系统
│   ├── 📱 mac_notifier.py         # Mac 通知
│   ├── 📋 data_handler.py         # 数据处理
│   └── 📝 logger.py               # 日志系统
├── 🎨 templates/                  # Web 模板
│   ├── 🏠 base.html               # 基础模板
│   ├── 🏠 index.html              # 多市场首页
│   ├── 🇭🇰 stocks.html            # 港股统计页面
│   ├── 🇭🇰 trades.html            # 港股交易记录页面
│   ├── 🇺🇸 us_stocks.html         # 美股统计页面
│   └── 🇺🇸 us_trades.html         # 美股交易记录页面
├── 💾 data/                       # 数据目录
│   ├── 🇭🇰 hk_options_monitor_v2.db # 港股 SQLite 数据库
│   └── 🇺🇸 us_options_monitor_v2.db # 美股 SQLite 数据库
├── 📝 logs/                       # 日志目录
└── 🧪 test_multi_db.py           # 多市场数据库测试工具
```

## 🔧 故障排除

### 常见问题

**Q: 富途 API 连接失败**
```
A: 1. 确保富途牛牛客户端已启动
   2. 检查 API 功能是否开启
   3. 验证 config.py 中的 host 和 port 配置
```

**Q: 没有收到通知**
```
A: 1. 检查通知配置是否正确
   2. 验证企业微信 webhook 地址
   3. 确认大单阈值设置是否合理
```

**Q: Web 界面无法访问**
```
A: 1. 确认 web_viewer.py 已启动
   2. 检查端口 5001 是否被占用
   3. 查看控制台错误信息
```

**Q: 多市场监控数据库重复初始化**
```
A: 1. 使用最新版本的多市场监控系统
   2. 确认使用 start_multi_market_monitor.py 启动
   3. 系统已优化为单例模式，每个市场只初始化一次数据库
```

**Q: 美股数据显示异常**
```
A: 1. 确认富途账户支持美股数据权限
   2. 检查美股交易时间设置
   3. 验证美股期权代码格式是否正确
```

## 🗓 TODO

- [x] ✅ **支持美股期权** - 已完成多市场期权监控
- [ ] 📊 **期权Greeks计算** - 添加Delta、Gamma、Theta等希腊字母计算
- [ ] 📈 **技术指标分析** - 集成常用技术指标
- [ ] 🤖 **AI智能预警** - 基于机器学习的异常交易识别
- [ ] 📱 **移动端适配** - 优化移动设备显示效果
- [ ] 🔄 **实时推送优化** - WebSocket实时数据推送
- [ ] 📊 **更多图表类型** - K线图、成交量分布图等

## 🤝 贡献指南

我们欢迎社区贡献！请遵循以下步骤：

1. **Fork** 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 **Pull Request**

## 📄 许可证

本项目基于 **GNU General Public License v3.0 or later** 开源协议发布。

```
Copyright 2025 V2 港股期权监控系统

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
```

详细许可证内容请查看 [LICENSE](LICENSE) 文件。

## 💖 赞助支持

如果这个项目对您有帮助，欢迎支持我们的开发工作！

### 🎯 **为什么需要您的支持？**

- 🔧 **持续开发**: 新功能开发和性能优化
- 🐛 **问题修复**: 及时响应和修复用户反馈的问题  
- 📚 **文档维护**: 保持文档的完整性和准确性
- 🚀 **服务器成本**: 维护演示环境和测试服务器

### 💰 **赞助方式**

#### 🇨🇳 **个人赞助**
<details>
<summary>点击展开二维码</summary>

**微信赞赏码**

![微信赞赏码](screenshots/wx.png)


**支付宝收款码**  

![支付宝收款码](screenshots/zfb.png)

</details>


#### 💎 **企业赞助**
如果您的企业希望成为项目赞助商，请联系我们讨论定制化支持和商业合作。

**联系方式**: liqian115@gmail.com

**联系wx**: altenli

### 🏆 **赞助者名单**

感谢以下赞助者对项目的支持：

| 赞助者 | 金额 | 时间 | 留言 |
|--------|------|------|------|
| 范 | ¥188 | 2025-9 | 期待您的支持！ |

> 💡 **注意**: 赞助完全自愿，项目将始终保持开源免费。您的支持将帮助我们提供更好的服务！

## 📞 联系我们

- **项目主页**: [GitHub 仓库链接](https://github.com/AltenLi/large_stock_options_monitor)
- **问题反馈**: [GitHub Issues 链接](https://github.com/AltenLi/large_stock_options_monitor/issues)
- **技术交流**: ![QQ群/微信群](screenshots/wxqun.jpg)
- **邮箱**: liqian115@gmail.com

## 🙏 致谢

- 感谢 [富途证券](https://www.futunn.com/) 提供的优秀 API 服务
- 感谢所有贡献者和用户的支持与反馈
- 感谢开源社区提供的优秀工具和库

---

<div align="center">

**⭐ 如果这个项目对您有帮助，请给我们一个 Star！⭐**

**🚀 让我们一起打造更好的港股期权监控工具！🚀**

</div>


---

## ⭐ Star History

[![Star History Chart](https://api.star-history.com/svg?repos=altenli/large_stock_options_monitor&type=Date)](https://star-history.com/#altenli/large_stock_options_monitor&Date)
