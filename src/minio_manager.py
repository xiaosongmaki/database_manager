import abc
import os
import logging
from typing import Optional, BinaryIO, List, Tuple
from pathlib import Path

from minio import Minio
from minio.error import S3Error

from src.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    MINIO_BUCKET
)


class StorageManager(abc.ABC):
    """抽象存储管理类"""
    
    @abc.abstractmethod
    def connect(self) -> None:
        """连接到存储服务"""
        pass
    
    @abc.abstractmethod
    def is_connected(self) -> bool:
        """检查是否已连接到存储服务"""
        pass
    
    @abc.abstractmethod
    def upload_file(self, 
                   local_path: str, 
                   remote_path: Optional[str] = None,
                   content_type: Optional[str] = None) -> str:
        """
        上传文件到存储服务
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程存储路径，如果为None则使用本地文件名
            content_type: 文件内容类型
            
        Returns:
            str: 远程文件的完整路径
        """
        pass
    
    @abc.abstractmethod
    def download_file(self, 
                     remote_path: str, 
                     local_path: Optional[str] = None) -> str:
        """
        从存储服务下载文件
        
        Args:
            remote_path: 远程存储路径
            local_path: 本地保存路径，如果为None则使用远程文件名保存到当前目录
            
        Returns:
            str: 本地文件的完整路径
        """
        pass
    
    @abc.abstractmethod
    def delete_file(self, remote_path: str) -> bool:
        """
        从存储服务删除文件
        
        Args:
            remote_path: 远程存储路径
            
        Returns:
            bool: 删除是否成功
        """
        pass
    
    @abc.abstractmethod
    def list_files(self, prefix: Optional[str] = None) -> List[Tuple[str, int]]:
        """
        列出存储服务中的文件
        
        Args:
            prefix: 文件前缀过滤器
            
        Returns:
            List[Tuple[str, int]]: 包含文件路径和大小的元组列表
        """
        pass


class MinioManager(StorageManager):
    """MinIO存储管理实现"""
    
    def __init__(self, 
                 endpoint: str = MINIO_ENDPOINT,
                 access_key: str = MINIO_ACCESS_KEY,
                 secret_key: str = MINIO_SECRET_KEY,
                 secure: bool = MINIO_SECURE,
                 bucket: str = MINIO_BUCKET):
        """
        初始化MinIO管理器
        
        Args:
            endpoint: MinIO服务端点
            access_key: 访问密钥
            secret_key: 密钥
            secure: 是否使用HTTPS
            bucket: 默认存储桶名称
        """
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.bucket = bucket
        self.client = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """连接到MinIO服务"""
        try:
            self.client = Minio(
                endpoint=self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure
            )
            
            # 检查bucket是否存在，如果不存在则创建
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                self.logger.info(f"创建MinIO存储桶: {self.bucket}")
                
            self.logger.info(f"成功连接到MinIO服务: {self.endpoint}")
        except S3Error as err:
            self.logger.error(f"连接MinIO服务失败: {err}")
            raise
    
    def is_connected(self) -> bool:
        """检查是否已连接到MinIO服务"""
        if self.client is None:
            return False
        
        try:
            # 尝试列出存储桶来验证连接
            self.client.bucket_exists(self.bucket)
            return True
        except:
            return False
    
    def upload_file(self, 
                   local_path: str, 
                   remote_path: Optional[str] = None,
                   content_type: Optional[str] = None) -> str:
        """
        上传文件到MinIO存储
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程存储路径，如果为None则使用本地文件名
            content_type: 文件内容类型
            
        Returns:
            str: 远程文件的完整路径
        """
        if not self.is_connected():
            self.connect()
        
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"本地文件不存在: {local_path}")
        
        # 如果未指定远程路径，则使用本地文件名
        if remote_path is None:
            remote_path = local_path.name
        
        # 确定内容类型
        if content_type is None:
            # 根据文件扩展名确定内容类型
            extension = local_path.suffix.lower()
            if extension == '.sql':
                content_type = 'application/sql'
            elif extension in ['.gz', '.gzip']:
                content_type = 'application/gzip'
            elif extension == '.zip':
                content_type = 'application/zip'
            else:
                content_type = 'application/octet-stream'
        
        try:
            # 获取文件大小
            file_size = os.path.getsize(local_path)
            
            # 上传文件
            self.logger.info(f"开始上传文件到MinIO: {remote_path}")
            
            result = self.client.fput_object(
                bucket_name=self.bucket,
                object_name=remote_path,
                file_path=str(local_path),
                content_type=content_type
            )
            
            self.logger.info(f"成功上传文件到MinIO: {result.object_name}, "
                            f"大小: {file_size} 字节, etag: {result.etag}")
            
            return result.object_name
            
        except S3Error as err:
            self.logger.error(f"上传文件到MinIO失败: {err}")
            raise
    
    def download_file(self, 
                     remote_path: str, 
                     local_path: Optional[str] = None) -> str:
        """
        从MinIO下载文件
        
        Args:
            remote_path: 远程存储路径
            local_path: 本地保存路径，如果为None则使用远程文件名保存到当前目录
            
        Returns:
            str: 本地文件的完整路径
        """
        if not self.is_connected():
            self.connect()
        
        # 如果未指定本地路径，则使用远程文件名
        if local_path is None:
            local_path = os.path.basename(remote_path)
        
        # 确保本地目录存在
        local_dir = os.path.dirname(local_path)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        
        try:
            # 下载文件
            self.logger.info(f"开始从MinIO下载文件: {remote_path}")
            
            self.client.fget_object(
                bucket_name=self.bucket,
                object_name=remote_path,
                file_path=local_path
            )
            
            self.logger.info(f"成功从MinIO下载文件: {remote_path} -> {local_path}")
            
            return local_path
            
        except S3Error as err:
            self.logger.error(f"从MinIO下载文件失败: {err}")
            raise
    
    def delete_file(self, remote_path: str) -> bool:
        """
        从MinIO删除文件
        
        Args:
            remote_path: 远程存储路径
            
        Returns:
            bool: 删除是否成功
        """
        if not self.is_connected():
            self.connect()
        
        try:
            # 删除文件
            self.logger.info(f"开始从MinIO删除文件: {remote_path}")
            
            self.client.remove_object(
                bucket_name=self.bucket,
                object_name=remote_path
            )
            
            self.logger.info(f"成功从MinIO删除文件: {remote_path}")
            
            return True
            
        except S3Error as err:
            self.logger.error(f"从MinIO删除文件失败: {err}")
            return False
    
    def list_files(self, prefix: Optional[str] = None) -> List[Tuple[str, int]]:
        """
        列出MinIO中的文件
        
        Args:
            prefix: 文件前缀过滤器
            
        Returns:
            List[Tuple[str, int]]: 包含文件路径和大小的元组列表
        """
        if not self.is_connected():
            self.connect()
        
        result = []
        
        try:
            # 列出对象
            objects = self.client.list_objects(
                bucket_name=self.bucket,
                prefix=prefix,
                recursive=True
            )
            
            for obj in objects:
                result.append((obj.object_name, obj.size))
            
            self.logger.info(f"从MinIO列出了 {len(result)} 个文件")
            
            return result
            
        except S3Error as err:
            self.logger.error(f"列出MinIO文件失败: {err}")
            raise
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = MinioManager()
    manager.connect()
    print("连接状态:", manager.is_connected())
    
    # 测试列出所有文件
    files = manager.list_files()
    print("MinIO文件列表:")
    for f, size in files:
        print(f"{f} ({size} bytes)")
    
    # 测试上传文件
    try:
        # 创建一个测试文件
        test_file = "test_upload_2.txt"
        with open(test_file, "w") as f:
            f.write("这是一个测试文件，用于验证MinIO上传功能")
        
        # 上传文件
        remote_path = manager.upload_file(test_file)
        print(f"文件已成功上传到: {remote_path}")
        
        # 再次列出文件，确认上传成功
        files = manager.list_files()
        print("上传后的文件列表:")
        for f, size in files:
            print(f"{f} ({size} bytes)")
            
        # 清理测试文件
        os.remove(test_file)
    except Exception as e:
        print(f"上传文件失败: {e}")
