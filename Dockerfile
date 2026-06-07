FROM python:3.11-slim
WORKDIR /app

# 先检查文件是否存在，再修改
RUN if [ -f /etc/apt/sources.list ]; then \
    sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list && \
    sed -i 's/security.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list; \
    fi

# 使用阿里云镜像安装mcp2mqtt
RUN pip install --no-cache-dir mcp2mqtt -i https://mirrors.aliyun.com/pypi/simple

CMD ["python", "-m", "mcp2mqtt.server"]