# 使用Python 3.12作为基础镜像
FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 设置时区
ENV TZ=Asia/Tokyo

# 安装系统依赖、MySQL客户端（包含mysqldump）和PostgreSQL客户端（包含pg_dump）
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-mysql-client \
    postgresql-client \
    tzdata \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 复制项目文件
COPY . /app/

# 升级pip
RUN pip install --upgrade pip

# 安装依赖 - 如果有pyproject.toml，使用pip安装项目
RUN pip install -e .

# 设置环境变量
ENV PYTHONPATH=/app 

CMD ["tail", "-f", "/dev/null"]