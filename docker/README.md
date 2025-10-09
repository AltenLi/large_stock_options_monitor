# V2多市场期权监控系统 Docker部署指南

## 快速开始

### 1. 环境准备

确保已安装：
- Docker (版本 20.0+)
- Docker Compose (版本 1.29+)

### 2. 配置设置

#### 方式一：使用环境变量（推荐）

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑环境变量文件
vim .env
```

#### 方式二：使用配置文件

```bash
# 创建配置目录
mkdir -p config

# 复制并编辑配置文件
cp v2_system/config.py.sample config/config.py
vim config/config.py
```

### 3. 启动服务

```bash
# 构建并启动容器
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f
```

### 4. 访问服务

- Web界面：http://localhost:5001
- 健康检查：http://localhost:5001/

## 高级配置

### 数据持久化

默认情况下，以下目录会被挂载到宿主机：
- `./v2_system/data` - 数据库文件
- `./v2_system/logs` - 日志文件
- `./config` - 配置文件（只读）

### 端口配置

如需修改Web服务端口，编辑 `docker-compose.yml`：

```yaml
ports:
  - "8080:5001"  # 将Web服务映射到8080端口
```

### 网络配置

系统内置了 FutuOpenD 服务，无需单独安装富途牛牛客户端。如需自定义配置：

```bash
# 设置 FutuOpenD 登录信息
export FUTU_LOGIN_ACCOUNT=your_account
export FUTU_LOGIN_PWD_MD5=your_md5_password

# 如果需要连接外部富途API服务
export FUTU_HOST=192.168.1.100
```

### FutuOpenD 配置

系统集成了 FutuOpenD 服务，支持以下配置：

- **FUTU_LOGIN_ACCOUNT**: 富途账号（默认：100000）
- **FUTU_LOGIN_PWD_MD5**: 登录密码的MD5值（默认：123456）

FutuOpenD 会自动启动并监听 11111 端口。

## 常用命令

```bash
# 启动服务
docker-compose up -d

# 停止服务
docker-compose down

# 重启服务
docker-compose restart

# 查看日志
docker-compose logs -f options-monitor

# 进入容器
docker-compose exec options-monitor bash

# 更新镜像
docker-compose pull
docker-compose up -d

# 清理数据（谨慎使用）
docker-compose down -v
```

## 故障排除

### 1. 容器启动失败

```bash
# 查看详细日志
docker-compose logs options-monitor

# 检查配置文件
docker-compose exec options-monitor cat /app/v2_system/config.py
```

### 2. 富途API连接失败

- 检查 FutuOpenD 服务是否正常启动
- 验证登录账号和密码配置
- 检查网络连接和防火墙设置
- 查看 FutuOpenD 日志：`docker-compose logs options-monitor | grep futu_opend`

### 3. Web界面无法访问

```bash
# 检查端口映射
docker-compose ps

# 检查Web服务日志
docker-compose logs options-monitor | grep web
```

### 4. 数据库问题

```bash
# 检查数据库文件权限
ls -la v2_system/data/

# 重置数据库（谨慎使用）
rm v2_system/data/*.db
docker-compose restart
```

## 监控和维护

### 健康检查

容器内置健康检查功能，可通过以下方式查看：

```bash
# 查看容器健康状态
docker-compose ps

# 手动执行健康检查
curl -f http://localhost:5001/
```

### 日志管理

日志文件会自动轮转，默认保留最近3个文件，每个文件最大100MB。

### 备份数据

```bash
# 备份数据库
tar -czf backup_$(date +%Y%m%d).tar.gz v2_system/data/

# 备份配置
tar -czf config_backup_$(date +%Y%m%d).tar.gz config/
```

## 性能优化

### 资源限制

在 `docker-compose.yml` 中添加资源限制：

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

使用SSD存储可以提高数据库性能：

```yaml
volumes:
  - type: bind
    source: /fast/storage/data
    target: /app/v2_system/data
```

## 安全建议

1. **不要在配置文件中硬编码密码**
2. **使用环境变量管理敏感信息**
3. **定期更新Docker镜像**
4. **限制容器网络访问**
5. **定期备份重要数据**