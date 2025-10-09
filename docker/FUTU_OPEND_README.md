# FutuOpenD Docker 集成说明

## 概述

本系统已集成 FutuOpenD 服务，无需单独安装富途牛牛客户端即可使用富途API功能。

## 配置说明

### 环境变量配置

在 `.env` 文件中配置以下参数：

```bash
# FutuOpenD 登录配置
FUTU_LOGIN_ACCOUNT=100000
FUTU_LOGIN_PWD_MD5=123456
```

### 获取MD5密码

如果需要使用MD5加密的密码，可以使用以下方法：

```bash
# 方法1：使用 echo 和 md5sum
echo -n "your_password" | md5sum

# 方法2：使用 Python
python3 -c "import hashlib; print(hashlib.md5('your_password'.encode()).hexdigest())"

# 方法3：在线MD5工具
# 访问 https://www.md5hashgenerator.com/
```

## 服务管理

### 启动服务

```bash
# 启动完整服务栈（包括 FutuOpenD）
docker-compose up -d

# 查看服务状态
docker-compose ps
```

### 查看日志

```bash
# 查看 FutuOpenD 日志
docker-compose logs -f options-monitor | grep futu_opend

# 查看所有服务日志
docker-compose logs -f
```

### 重启 FutuOpenD

```bash
# 重启整个容器
docker-compose restart options-monitor

# 或者进入容器手动重启
docker-compose exec options-monitor supervisorctl restart futu_opend
```

## 端口说明

- **5001**: Web界面端口
- **11111**: FutuOpenD API端口

## 配置文件

FutuOpenD 的配置文件位于容器内的 `/app/futu_opend/FutuOpenD.xml`，启动时会根据环境变量自动配置。

## 故障排除

### 1. FutuOpenD 启动失败

```bash
# 检查日志
docker-compose logs options-monitor | grep futu_opend

# 检查配置文件
docker-compose exec options-monitor cat /app/futu_opend/FutuOpenD.xml

# 检查进程状态
docker-compose exec options-monitor supervisorctl status futu_opend
```

### 2. 登录失败

- 确认账号和密码正确
- 检查是否需要使用MD5加密密码
- 验证富途账户是否有API权限

### 3. 连接超时

- 检查防火墙设置
- 确认端口 11111 未被占用
- 验证网络连接

## 高级配置

### 自定义配置文件

如需自定义 FutuOpenD 配置，可以：

1. 创建自定义配置文件
2. 挂载到容器中
3. 修改启动脚本

```yaml
# docker-compose.yml
volumes:
  - ./custom_futu_config.xml:/app/futu_opend/FutuOpenD.xml:ro
```

### 使用外部 FutuOpenD

如果已有外部 FutuOpenD 服务，可以：

1. 停用内置 FutuOpenD
2. 配置连接外部服务

```bash
# 停用内置服务
docker-compose exec options-monitor supervisorctl stop futu_opend

# 配置外部连接
export FUTU_HOST=external_host_ip
```

## 安全建议

1. **不要在配置文件中明文存储密码**
2. **使用环境变量管理敏感信息**
3. **定期更换登录密码**
4. **限制API访问权限**
5. **监控异常登录活动**