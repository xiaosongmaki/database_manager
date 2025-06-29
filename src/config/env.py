import os
from dotenv import load_dotenv

# 加载.env文件
load_dotenv()

# MySQL配置
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT',3306))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

# PostgreSQL配置
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', 5432))
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')

# MinIO配置
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'False').lower() == 'true'
MINIO_BUCKET = os.getenv('MINIO_BUCKET')


# 测试MySQL配置
TEST_MYSQL_HOST = os.getenv('TEST_MYSQL_HOST')
TEST_MYSQL_PORT = int(os.getenv('TEST_MYSQL_PORT',3306))
TEST_MYSQL_USER = os.getenv('TEST_MYSQL_USER')
TEST_MYSQL_PASSWORD = os.getenv('TEST_MYSQL_PASSWORD')
TEST_MYSQL_DATABASE = os.getenv('TEST_MYSQL_DATABASE')


# 备份配置
BACKUP_DIR = '/tmp/backups'
