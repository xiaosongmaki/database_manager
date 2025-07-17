import abc
import os
import subprocess
import datetime
import logging
import mysql.connector
import psycopg2
import pymongo
from pathlib import Path
from typing import Optional, Dict, Any, Union

from src.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_DATABASE,
)

class DatabaseManager(abc.ABC):
    """抽象数据库管理类"""
    
    @abc.abstractmethod
    def connect(self) -> None:
        """连接到数据库"""
        pass
    
    @abc.abstractmethod
    def disconnect(self) -> None:
        """断开数据库连接"""
        pass
    
    @abc.abstractmethod
    def is_connected(self) -> bool:
        """检查是否已连接到数据库"""
        pass
    
    @abc.abstractmethod
    def dump(self, 
             output_dir: str, 
             lock_tables: bool = False,
             filename: Optional[str] = None,
             extra_options: Optional[Dict[str, Any]] = None,
             all_databases: bool = False) -> str:
        """
        导出数据库
        
        Args:
            output_dir: 导出文件保存目录
            lock_tables: 是否在导出时锁表，默认不锁表（可能导致数据不一致）
            filename: 自定义导出文件名，如果为None则自动生成
            extra_options: 额外的导出选项
            all_databases: 是否导出所有数据库，默认为False
            
        Returns:
            str: 导出文件的完整路径
        """
        pass
    
    @abc.abstractmethod
    def restore(self,
               sql_file: str,
               extra_options: Optional[Dict[str, Any]] = None) -> bool:
        """
        从SQL文件恢复数据库
        
        Args:
            sql_file: SQL备份文件的路径
            extra_options: 额外的恢复选项
            
        Returns:
            bool: 恢复是否成功
        """
        pass


class MySQLManager(DatabaseManager):
    """MySQL数据库管理实现"""
    
    def __init__(self, 
                 host: str = MYSQL_HOST,
                 port: int = MYSQL_PORT,
                 user: str = MYSQL_USER,
                 password: str = MYSQL_PASSWORD,
                 database: str = MYSQL_DATABASE):
        """
        初始化MySQL管理器
        
        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """连接到MySQL数据库"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.logger.info(f"成功连接到MySQL数据库: {self.host}:{self.port}/{self.database}")
        except mysql.connector.Error as err:
            self.logger.error(f"连接MySQL数据库失败: {err}")
            raise
    
    def disconnect(self) -> None:
        """断开MySQL数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.connection = None
            self.logger.info("已断开MySQL数据库连接")
    
    def is_connected(self) -> bool:
        """检查是否已连接到MySQL数据库"""
        return self.connection is not None and self.connection.is_connected()
    
    def dump(self, 
             output_dir: str, 
             lock_tables: bool = False,
             filename: Optional[str] = None,
             extra_options: Optional[Dict[str, Any]] = None,
             all_databases: bool = False) -> str:
        """
        使用mysqldump导出MySQL数据库
        
        Args:
            output_dir: 导出文件保存目录
            lock_tables: 是否在导出时锁表，默认不锁表（可能导致数据不一致）
            filename: 自定义导出文件名，如果为None则自动生成
            extra_options: 额外的导出选项
            all_databases: 是否导出所有数据库，默认为False
            
        Returns:
            str: 导出文件的完整路径
        """
        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        if filename is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            if all_databases:
                filename = f"all_databases_{timestamp}.sql"
            else:
                filename = f"{self.database}_{timestamp}.sql"
        
        # 完整输出路径
        output_path = os.path.join(output_dir, filename)
        
        # 构建mysqldump命令
        cmd = [
            "mysqldump",
            f"--host={self.host}",
            f"--port={self.port}",
            f"--user={self.user}",
            f"--password={self.password}",
            "--single-transaction",  # 保证备份一致性
            "--protocol=TCP"  # 确保使用TCP/IP连接
        ]
        
        # 是否锁表
        if not lock_tables:
            cmd.append("--skip-lock-tables")
        else:
            cmd.append("--lock-tables")
        
        # 添加额外选项
        if extra_options:
            for key, value in extra_options.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.append(f"--{key}={value}")
        
        # 是否导出所有数据库
        if all_databases:
            cmd.append("--all-databases")
            self.logger.info("准备导出所有数据库")
        else:
            # 添加数据库名称
            cmd.append(self.database)
        
        try:
            # 执行mysqldump命令并将输出重定向到文件
            with open(output_path, 'w') as f:
                if all_databases:
                    self.logger.info("开始导出所有MySQL数据库")
                else:
                    self.logger.info(f"开始导出MySQL数据库: {self.database}")
                self.logger.debug(f"执行命令: {' '.join(cmd)}")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                _, stderr = process.communicate()
                
                if process.returncode != 0:
                    self.logger.error(f"导出MySQL数据库失败: {stderr}")
                    raise Exception(f"导出MySQL数据库失败: {stderr}")
                
                if all_databases:
                    self.logger.info(f"成功导出所有MySQL数据库到: {output_path}")
                else:
                    self.logger.info(f"成功导出MySQL数据库到: {output_path}")
                return output_path
                
        except Exception as e:
            self.logger.error(f"导出MySQL数据库时发生错误: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            raise
    
    def restore(self,
               sql_file: str,
               extra_options: Optional[Dict[str, Any]] = None) -> bool:
        """
        使用mysql命令从SQL文件恢复MySQL数据库
        
        Args:
            sql_file: SQL备份文件的路径
            extra_options: 额外的恢复选项
            
        Returns:
            bool: 恢复是否成功
        """
        if not os.path.exists(sql_file):
            self.logger.error(f"SQL备份文件不存在: {sql_file}")
            raise FileNotFoundError(f"SQL备份文件不存在: {sql_file}")
        
        # 构建mysql命令
        cmd = [
            "mysql",
            f"--host={self.host}",
            f"--port={self.port}",
            f"--user={self.user}",
            f"--password={self.password}",
            "--protocol=TCP"  # 确保使用TCP/IP连接
        ]
        
        # 添加额外选项
        if extra_options:
            for key, value in extra_options.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.append(f"--{key}={value}")
        
        # 添加数据库名称
        cmd.append(self.database)
        
        try:
            # 执行mysql命令从SQL文件恢复数据库
            self.logger.info(f"开始从SQL文件恢复MySQL数据库: {self.database}")
            self.logger.debug(f"执行命令: {' '.join(cmd)} < {sql_file}")
            
            with open(sql_file, 'r') as f:
                process = subprocess.Popen(
                    cmd,
                    stdin=f,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                _, stderr = process.communicate()
                
                if process.returncode != 0:
                    self.logger.error(f"恢复MySQL数据库失败: {stderr}")
                    raise Exception(f"恢复MySQL数据库失败: {stderr}")
                
                self.logger.info(f"成功从SQL文件恢复MySQL数据库: {self.database}")
                return True
                
        except Exception as e:
            self.logger.error(f"恢复MySQL数据库时发生错误: {str(e)}")
            raise


class PostgreSQLManager(DatabaseManager):
    """PostgreSQL数据库管理实现"""
    
    def __init__(self, 
                 host: str = POSTGRES_HOST,
                 port: int = POSTGRES_PORT,
                 user: str = POSTGRES_USER,
                 password: str = POSTGRES_PASSWORD,
                 database: str = POSTGRES_DATABASE):
        """
        初始化PostgreSQL管理器
        
        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """连接到PostgreSQL数据库"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.database
            )
            self.logger.info(f"成功连接到PostgreSQL数据库: {self.host}:{self.port}/{self.database}")
        except psycopg2.Error as err:
            self.logger.error(f"连接PostgreSQL数据库失败: {err}")
            raise
    
    def disconnect(self) -> None:
        """断开PostgreSQL数据库连接"""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("已断开PostgreSQL数据库连接")
    
    def is_connected(self) -> bool:
        """检查是否已连接到PostgreSQL数据库"""
        if self.connection is None:
            return False
        
        try:
            # 尝试执行一个简单查询来验证连接
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except:
            return False
    
    def dump(self, 
             output_dir: str, 
             lock_tables: bool = False,  # 在PostgreSQL中不使用，保留参数兼容性
             filename: Optional[str] = None,
             extra_options: Optional[Dict[str, Any]] = None,
             all_databases: bool = True) -> str:
        """
        使用pg_dump导出PostgreSQL数据库
        
        Args:
            output_dir: 导出文件保存目录
            lock_tables: 不适用于PostgreSQL，保留参数兼容性
            filename: 自定义导出文件名，如果为None则自动生成
            extra_options: 额外的导出选项
            all_databases: 是否导出所有数据库，默认为False
            
        Returns:
            str: 导出文件的完整路径
        """
        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        if filename is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            if all_databases:
                filename = f"all_databases_{timestamp}.sql"
            else:
                filename = f"{self.database}_{timestamp}.sql"
        
        # 完整输出路径
        output_path = os.path.join(output_dir, filename)
        
        # 如果是导出所有数据库，使用pg_dumpall而不是pg_dump
        if all_databases:
            cmd = [
                "pg_dumpall",
                f"--host={self.host}",
                f"--port={self.port}",
                f"--username={self.user}",
                "--clean",  # 添加删除数据库对象的命令
                "--if-exists",  # 使用IF EXISTS删除对象
                "--no-owner"  # 不输出设置对象所有权的命令
            ]
            self.logger.info("准备导出所有PostgreSQL数据库")
        else:
            # 构建pg_dump命令
            cmd = [
                "pg_dump",
                f"--host={self.host}",
                f"--port={self.port}",
                f"--username={self.user}",
                "--format=plain",  # 纯文本SQL格式
                "--clean",  # 添加删除数据库对象的命令
                "--if-exists",  # 使用IF EXISTS删除对象
                "--no-owner"  # 不输出设置对象所有权的命令
            ]
            
            # 添加数据库名称（仅当不是导出所有数据库时）
            cmd.append(self.database)
        
        # 添加额外选项
        if extra_options:
            for key, value in extra_options.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.append(f"--{key}={value}")
        
        try:
            # 设置环境变量PGPASSWORD
            env = os.environ.copy()
            env['PGPASSWORD'] = self.password
            
            # 执行pg_dump/pg_dumpall命令并将输出重定向到文件
            with open(output_path, 'w') as f:
                if all_databases:
                    self.logger.info("开始导出所有PostgreSQL数据库")
                else:
                    self.logger.info(f"开始导出PostgreSQL数据库: {self.database}")
                self.logger.debug(f"执行命令: {' '.join(cmd)}")
                
                process = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    env=env,
                    universal_newlines=True
                )
                
                _, stderr = process.communicate()
                
                if process.returncode != 0:
                    self.logger.error(f"导出PostgreSQL数据库失败: {stderr}")
                    raise Exception(f"导出PostgreSQL数据库失败: {stderr}")
                
                if all_databases:
                    self.logger.info(f"成功导出所有PostgreSQL数据库到: {output_path}")
                else:
                    self.logger.info(f"成功导出PostgreSQL数据库到: {output_path}")
                return output_path
                
        except Exception as e:
            self.logger.error(f"导出PostgreSQL数据库时发生错误: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            raise
    
    def restore(self,
               sql_file: str,
               extra_options: Optional[Dict[str, Any]] = None) -> bool:
        """
        使用psql命令从SQL文件恢复PostgreSQL数据库
        
        Args:
            sql_file: SQL备份文件的路径
            extra_options: 额外的恢复选项
            
        Returns:
            bool: 恢复是否成功
        """
        if not os.path.exists(sql_file):
            self.logger.error(f"SQL备份文件不存在: {sql_file}")
            raise FileNotFoundError(f"SQL备份文件不存在: {sql_file}")
        
        # 构建psql命令
        cmd = [
            "psql",
            f"--host={self.host}",
            f"--port={self.port}",
            f"--username={self.user}",
            "--set=ON_ERROR_STOP=on",  # 在遇到错误时停止执行
            "--no-password"  # 不提示输入密码（使用环境变量）
        ]
        
        # 添加额外选项
        if extra_options:
            for key, value in extra_options.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.append(f"--{key}={value}")
        
        # 添加数据库名称
        cmd.append(self.database)
        
        try:
            # 设置环境变量PGPASSWORD
            env = os.environ.copy()
            env['PGPASSWORD'] = self.password
            
            # 执行psql命令从SQL文件恢复数据库
            self.logger.info(f"开始从SQL文件恢复PostgreSQL数据库: {self.database}")
            self.logger.debug(f"执行命令: {' '.join(cmd)} < {sql_file}")
            
            with open(sql_file, 'r') as f:
                process = subprocess.Popen(
                    cmd,
                    stdin=f,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    universal_newlines=True
                )
                
                _, stderr = process.communicate()
                
                if process.returncode != 0:
                    self.logger.error(f"恢复PostgreSQL数据库失败: {stderr}")
                    raise Exception(f"恢复PostgreSQL数据库失败: {stderr}")
                
                self.logger.info(f"成功从SQL文件恢复PostgreSQL数据库: {self.database}")
                return True
                
        except Exception as e:
            self.logger.error(f"恢复PostgreSQL数据库时发生错误: {str(e)}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = MySQLManager()
    manager.connect()
    print("连接状态:", manager.is_connected())
    manager.dump(output_dir="./tmp/mysql_dumps",filename="test.sql")



class MongoDBManager(DatabaseManager):
    """MongoDB数据库管理实现"""
    
    def __init__(self, 
                 host: str = MONGODB_HOST,
                 port: int = MONGODB_PORT,
                 user: str = MONGODB_USER,
                 password: str = MONGODB_PASSWORD,
                 database: str = MONGODB_DATABASE,
                 auth_database: str = MONGODB_AUTH_DATABASE):
        """
        初始化MongoDB管理器
        
        Args:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            auth_database: 认证数据库名称
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.auth_database = auth_database
        self.client = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """连接到MongoDB数据库"""
        try:
            # 构建连接字符串
            if self.user and self.password:
                connection_string = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.auth_database}"
            else:
                connection_string = f"mongodb://{self.host}:{self.port}/"
            
            self.client = pymongo.MongoClient(connection_string)
            
            # 测试连接
            self.client.admin.command('ping')
            self.logger.info(f"成功连接到MongoDB数据库: {self.host}:{self.port}/{self.database}")
        except pymongo.errors.PyMongoError as err:
            self.logger.error(f"连接MongoDB数据库失败: {err}")
            raise
    
    def disconnect(self) -> None:
        """断开MongoDB数据库连接"""
        if self.client:
            self.client.close()
            self.client = None
            self.logger.info("已断开MongoDB数据库连接")
    
    def is_connected(self) -> bool:
        """检查是否已连接到MongoDB数据库"""
        if self.client is None:
            return False
        
        try:
            # 尝试执行一个简单命令来验证连接
            self.client.admin.command('ping')
            return True
        except:
            return False
    
    def dump(self, 
             output_dir: str, 
             lock_tables: bool = False,  # MongoDB中不适用，保留参数兼容性
             filename: Optional[str] = None,
             extra_options: Optional[Dict[str, Any]] = None,
             all_databases: bool = False) -> str:
        """
        使用mongodump导出MongoDB数据库
        
        Args:
            output_dir: 导出文件保存目录
            lock_tables: 不适用于MongoDB，保留参数兼容性
            filename: 自定义导出文件名，如果为None则自动生成
            extra_options: 额外的导出选项
            all_databases: 是否导出所有数据库，默认为False
            
        Returns:
            str: 导出文件的完整路径
        """
        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        if filename is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            if all_databases:
                filename = f"all_databases_{timestamp}"
            else:
                filename = f"{self.database}_{timestamp}"
        else:
            # 移除可能的文件扩展名，因为mongodump创建目录
            filename = os.path.splitext(filename)[0]
        
        # 完整输出路径
        output_path = os.path.join(output_dir, filename)
        
        # 构建mongodump命令
        cmd = [
            "mongodump",
            "--host", f"{self.host}:{self.port}",
            "--out", output_path
        ]
        
        # 添加认证信息
        if self.user and self.password:
            cmd.extend(["--username", self.user])
            cmd.extend(["--password", self.password])
            cmd.extend(["--authenticationDatabase", self.auth_database])
        
        # 是否导出所有数据库
        if not all_databases and self.database:
            cmd.extend(["--db", self.database])
        
        # 添加额外选项
        if extra_options:
            for key, value in extra_options.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.extend([f"--{key}", str(value)])
        
        try:
            # 执行mongodump命令
            if all_databases:
                self.logger.info("开始导出所有MongoDB数据库")
            else:
                self.logger.info(f"开始导出MongoDB数据库: {self.database}")
            self.logger.debug(f"执行命令: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                self.logger.error(f"导出MongoDB数据库失败: {stderr}")
                raise Exception(f"导出MongoDB数据库失败: {stderr}")
            
            if all_databases:
                self.logger.info(f"成功导出所有MongoDB数据库到: {output_path}")
            else:
                self.logger.info(f"成功导出MongoDB数据库到: {output_path}")
            
            # 创建一个tar.gz压缩文件以便于存储和传输
            import tarfile
            tar_path = f"{output_path}.tar.gz"
            with tarfile.open(tar_path, "w:gz") as tar:
                tar.add(output_path, arcname=os.path.basename(output_path))
            
            # 删除原始目录
            import shutil
            shutil.rmtree(output_path)
            
            self.logger.info(f"已创建压缩备份文件: {tar_path}")
            return tar_path
            
        except Exception as e:
            self.logger.error(f"导出MongoDB数据库时发生错误: {str(e)}")
            if os.path.exists(output_path):
                import shutil
                shutil.rmtree(output_path, ignore_errors=True)
            raise
    
    def restore(self,
               sql_file: str,  # 对于MongoDB，这实际上是备份目录或tar.gz文件
               extra_options: Optional[Dict[str, Any]] = None) -> bool:
        """
        使用mongorestore从备份文件恢复MongoDB数据库
        
        Args:
            sql_file: 备份文件的路径（可以是目录或tar.gz文件）
            extra_options: 额外的恢复选项
            
        Returns:
            bool: 恢复是否成功
        """
        if not os.path.exists(sql_file):
            self.logger.error(f"备份文件不存在: {sql_file}")
            raise FileNotFoundError(f"备份文件不存在: {sql_file}")
        
        # 处理压缩文件
        restore_path = sql_file
        temp_dir = None
        
        if sql_file.endswith('.tar.gz'):
            import tarfile
            import tempfile
            
            # 创建临时目录解压文件
            temp_dir = tempfile.mkdtemp()
            with tarfile.open(sql_file, "r:gz") as tar:
                tar.extractall(temp_dir)
            
            # 找到解压后的目录
            extracted_items = os.listdir(temp_dir)
            if len(extracted_items) == 1:
                restore_path = os.path.join(temp_dir, extracted_items[0])
            else:
                restore_path = temp_dir
        
        # 构建mongorestore命令
        cmd = [
            "mongorestore",
            "--host", f"{self.host}:{self.port}"
        ]
        
        # 添加认证信息
        if self.user and self.password:
            cmd.extend(["--username", self.user])
            cmd.extend(["--password", self.password])
            cmd.extend(["--authenticationDatabase", self.auth_database])
        
        # 添加额外选项
        if extra_options:
            for key, value in extra_options.items():
                if value is True:
                    cmd.append(f"--{key}")
                elif value is not False and value is not None:
                    cmd.extend([f"--{key}", str(value)])
        
        # 如果指定了数据库，添加数据库选项
        if self.database and os.path.isdir(os.path.join(restore_path, self.database)):
            cmd.extend(["--db", self.database])
            cmd.append(os.path.join(restore_path, self.database))
        else:
            cmd.append(restore_path)
        
        try:
            # 执行mongorestore命令
            self.logger.info(f"开始从备份文件恢复MongoDB数据库: {self.database}")
            self.logger.debug(f"执行命令: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                self.logger.error(f"恢复MongoDB数据库失败: {stderr}")
                raise Exception(f"恢复MongoDB数据库失败: {stderr}")
            
            self.logger.info(f"成功从备份文件恢复MongoDB数据库: {self.database}")
            return True
            
        except Exception as e:
            self.logger.error(f"恢复MongoDB数据库时发生错误: {str(e)}")
            raise
        finally:
            # 清理临时目录
            if temp_dir and os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)

