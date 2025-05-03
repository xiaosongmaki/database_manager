import abc
import os
import subprocess
import datetime
import logging
import mysql.connector
from pathlib import Path
from typing import Optional, Dict, Any

from src.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE
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
             extra_options: Optional[Dict[str, Any]] = None) -> str:
        """
        导出数据库
        
        Args:
            output_dir: 导出文件保存目录
            lock_tables: 是否在导出时锁表，默认不锁表（可能导致数据不一致）
            filename: 自定义导出文件名，如果为None则自动生成
            extra_options: 额外的导出选项
            
        Returns:
            str: 导出文件的完整路径
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
             extra_options: Optional[Dict[str, Any]] = None) -> str:
        """
        使用mysqldump导出MySQL数据库
        
        Args:
            output_dir: 导出文件保存目录
            lock_tables: 是否在导出时锁表，默认不锁表（可能导致数据不一致）
            filename: 自定义导出文件名，如果为None则自动生成
            extra_options: 额外的导出选项
            
        Returns:
            str: 导出文件的完整路径
        """
        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        if filename is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
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
        
        # 添加数据库名称
        cmd.append(self.database)
        
        try:
            # 执行mysqldump命令并将输出重定向到文件
            with open(output_path, 'w') as f:
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
                
                self.logger.info(f"成功导出MySQL数据库到: {output_path}")
                return output_path
                
        except Exception as e:
            self.logger.error(f"导出MySQL数据库时发生错误: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            raise




if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = MySQLManager()
    manager.connect()
    print("连接状态:", manager.is_connected())
    manager.dump(output_dir="./tmp/mysql_dumps",filename="test.sql")
