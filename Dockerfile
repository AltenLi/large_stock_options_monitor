# V2多市场期权监控系统 Docker镜像
FROM continuumio/miniconda3:latest

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    supervisor \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 创建Python 3.11环境
RUN conda create -n options_monitor python=3.11 -y

# 激活环境并设置为默认
SHELL ["conda", "run", "-n", "options_monitor", "/bin/bash", "-c"]

# 复制项目文件
COPY . /app/

# 复制 FutuOpenD 并设置执行权限
COPY Futu_OpenD_9.4.5418_Centos7/ /app/futu_opend/
RUN chmod +x /app/futu_opend/FutuOpenD

# 安装Python依赖
RUN conda run -n options_monitor pip install --no-cache-dir -r v2_system/requirements.txt

# 创建必要的目录
RUN mkdir -p /app/v2_system/data \
    /app/v2_system/logs \
    /app/v2_system/data/cache \
    /app/config \
    /app/scripts

# 复制配置文件模板
RUN if [ ! -f /app/v2_system/config.py ]; then \
        cp /app/v2_system/config.py.sample /app/v2_system/config.py; \
    fi

# 创建启动脚本
COPY docker/start.sh /app/scripts/start.sh
COPY docker/configure_futu_opend.sh /app/docker/configure_futu_opend.sh
COPY docker/start_futu_opend.sh /app/docker/start_futu_opend.sh
COPY docker/test_futu_opend.sh /app/docker/test_futu_opend.sh
COPY docker/supervisor.conf /etc/supervisor/conf.d/options_monitor.conf

# 设置脚本执行权限
RUN chmod +x /app/scripts/start.sh /app/docker/configure_futu_opend.sh /app/docker/start_futu_opend.sh /app/docker/test_futu_opend.sh

# 暴露端口
EXPOSE 5001 11111

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:5001/ || exit 1

# 设置数据卷
VOLUME ["/app/v2_system/data", "/app/v2_system/logs", "/app/config"]

# 启动命令
CMD ["/app/scripts/start.sh"]