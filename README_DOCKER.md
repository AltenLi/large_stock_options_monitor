# V2多市场期权监控系统 Docker部署指南

## 🚀 快速开始

### 一键部署

```bash
# 1. 克隆项目
git clone <repository-url>
cd large_stock_options_monitor

# 2. 构建并启动
chmod +x build.sh deploy.sh
./deploy.sh start
```

### 访问服务

- **Web界面**: http://localhost:5001
- **健康检查**: http://localhost:5001/

## 📋 详细部署步骤

### 1. 环境准备

确保系统已安装：
- Docker (20.0+)
- Docker Compose (1.29+)

```bash
# 检查Docker版本
docker --version
docker-compose --version
```

### 2. 配置设置

#### 方式一：环境变量配置（推荐）

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑配置
vim .env
```

配置示例：
```bash
# 富途API配置
FUTU_HOST=127.0.0.1
FUTU_PORT=11111
FUTU_UNLOCK_PWD=your_password

# 企业微信通知
WEWORK_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
```

#### 方式二：配置文件

```bash
# 创建配置目录并复制配置文件
mkdir -p config
cp v2_system/config.py.sample config/config.py
vim config/config.py
```

### 3. 构建和启动

#### 使用部署脚本（推荐）

```bash
# 赋予执行权限
chmod +x deploy.sh

# 启动服务
./deploy.sh start

# 查看状态
./deploy.sh status

# 查看日志
./deploy.sh logs
```

#### 使用Docker Compose

```bash
# 构建镜像
docker-compose build

# 启动服务
docker-compose up -d

# 查看状态
docker-compose ps
```

## 🛠️ 管理命令

### 部署脚本命令

```bash
./deploy.sh start     # 启动服务
./deploy.sh stop      # 停止服务
./deploy.sh restart   # 重启服务
./deploy.sh status    # 查看状态
./deploy.sh logs      # 查看日志
./deploy.sh build     # 构建镜像
./deploy.sh clean     # 清理资源
```

### Docker Compose命令

```bash
# 服务管理
docker-compose up -d              # 启动服务
docker-compose down               # 停止服务
docker-compose restart           # 重启服务
docker-compose ps                # 查看状态

# 日志管理
docker-compose logs -f           # 查看所有日志
docker-compose logs options-monitor  # 查看特定服务日志

# 容器操作
docker-compose exec options-monitor bash  # 进入容器
docker-compose exec options-monitor supervisorctl status  # 查看进程状态
```

## 📊 监控和维护

### 健康检查

```bash
# 手动健康检查
curl -f http://localhost:5001/

# 容器内健康检查
docker-compose exec options-monitor /app/docker/health-check.sh
```

### 查看进程状态

```bash
# 查看supervisor管理的进程
docker-compose exec options-monitor supervisorctl status

# 重启特定进程
docker-compose exec options-monitor supervisorctl restart multi_market_monitor
docker-compose exec options-monitor supervisorctl restart web_viewer
```

### 日志管理

```bash
# 查看不同类型的日志
docker-compose exec options-monitor tail -f /app/v2_system/logs/monitor.log
docker-compose exec options-monitor tail -f /app/v2_system/logs/web.log
docker-compose exec options-monitor tail -f /app/v2_system/logs/supervisord.log
```

## 🔧 故障排除

### 常见问题

#### 1. 容器启动失败

```bash
# 查看详细错误信息
docker-compose logs options-monitor

# 检查配置文件
docker-compose exec options-monitor cat /app/v2_system/config.py
```

#### 2. 富途API连接失败

- 确保富途牛牛客户端已启动并开启API功能
- 检查网络连接和防火墙设置
- 验证API配置参数

```bash
# 测试网络连接
docker-compose exec options-monitor ping futu_host_ip
```

#### 3. Web界面无法访问

```bash
# 检查端口映射
docker-compose ps

# 检查Web进程状态
docker-compose exec options-monitor supervisorctl status web_viewer

# 手动启动Web服务测试
docker-compose exec options-monitor python /app/v2_system/web_viewer.py
```

#### 4. 数据库问题

```bash
# 检查数据库文件
docker-compose exec options-monitor ls -la /app/v2_system/data/

# 检查数据库权限
docker-compose exec options-monitor sqlite3 /app/v2_system/data/hk_options_monitor_v2.db ".tables"
```

### 重置和恢复

```bash
# 重置所有数据（谨慎使用）
docker-compose down -v
rm -rf v2_system/data/* v2_system/logs/*
docker-compose up -d

# 仅重启服务
./deploy.sh restart
```

## 📈 性能优化

### 资源限制

编辑 `docker-compose.yml` 添加资源限制：

```yaml
deploy:
  resources:
    limits:
      cpus: '2.0'
      memory: 2G
    reservations:
      cpus: '0.5'
      memory: 512M
```

### 存储优化

```bash
# 使用SSD存储提高性能
mkdir -p /fast/storage/options-monitor
ln -s /fast/storage/options-monitor v2_system/data
```

## 🔒 安全建议

1. **配置安全**
   - 使用环境变量管理敏感信息
   - 不要在代码中硬编码密码
   - 定期更新配置文件权限

2. **网络安全**
   - 限制容器网络访问
   - 使用防火墙规则
   - 定期更新Docker镜像

3. **数据安全**
   - 定期备份重要数据
   - 使用加密存储
   - 监控异常访问

## 📦 备份和恢复

### 数据备份

```bash
# 备份数据库
tar -czf backup_$(date +%Y%m%d_%H%M%S).tar.gz v2_system/data/

# 备份配置
tar -czf config_backup_$(date +%Y%m%d_%H%M%S).tar.gz config/ .env
```

### 数据恢复

```bash
# 停止服务
./deploy.sh stop

# 恢复数据
tar -xzf backup_YYYYMMDD_HHMMSS.tar.gz

# 启动服务
./deploy.sh start
```

## 🆙 更新升级

```bash
# 拉取最新代码
git pull

# 重新构建镜像
./deploy.sh build

# 重启服务
./deploy.sh restart
```

## 📞 技术支持

如遇问题，请：

1. 查看日志文件获取详细错误信息
2. 检查配置文件是否正确
3. 参考故障排除章节
4. 提交Issue到项目仓库