import os
import datetime
import shutil
from typing import Optional, Dict, Any, List
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from src.database_manager import DatabaseManager
from src.minio_manager import StorageManager
from src.config import BACKUP_DIR


class DatabaseBackup:
    """数据库备份类"""
    
    def __init__(self, 
                 db_manager: DatabaseManager,
                 storage_manager: StorageManager,
                 backup_dir: str = BACKUP_DIR):
        """
        初始化数据库备份
        
        Args:
            db_manager: 数据库管理器
            storage_manager: 存储管理器
            backup_dir: 临时备份目录
        """
        self.db_manager = db_manager
        self.storage_manager = storage_manager
        self.backup_dir = backup_dir
        self.logger = get_run_logger()
        
        # 确保备份目录存在
        os.makedirs(self.backup_dir, exist_ok=True)
    
    def create_backup(self, 
                     lock_tables: bool = False,
                     remote_path: Optional[str] = None,
                     extra_options: Optional[Dict[str, Any]] = None,
                     remove_local: bool = True) -> str:
        """
        创建数据库备份并上传到存储服务
        
        Args:
            lock_tables: 是否在导出时锁表，默认不锁表（可能导致数据不一致）
            remote_path: 远程存储路径，如果为None则自动生成
            extra_options: 额外的导出选项
            remove_local: 备份完成后是否删除本地文件
            
        Returns:
            str: 备份在存储服务中的路径
        """
        try:
            # 连接数据库
            if not self.db_manager.is_connected():
                self.db_manager.connect()
                
            # 连接存储服务
            if not self.storage_manager.is_connected():
                self.storage_manager.connect()
            
            # 导出数据库
            local_backup_path = self.db_manager.dump(
                output_dir=self.backup_dir,
                lock_tables=lock_tables,
                extra_options=extra_options
            )
            
            # 生成远程路径
            if remote_path is None:
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                remote_path = f"backup_{timestamp}.sql"
            
            # 上传到存储服务
            storage_path = self.storage_manager.upload_file(
                local_path=local_backup_path,
                remote_path=remote_path
            )
            
            # 如果需要，删除本地文件
            if remove_local and os.path.exists(local_backup_path):
                os.remove(local_backup_path)
                self.logger.info(f"已删除本地备份文件: {local_backup_path}")
            
            self.logger.info(f"数据库备份完成: {storage_path}")
            return storage_path
            
        except Exception as e:
            self.logger.error(f"数据库备份失败: {str(e)}")
            raise
    
    def restore_backup(self, 
                      backup_path: str,
                      local_path: Optional[str] = None,
                      remove_local: bool = True) -> bool:
        """
        从备份恢复数据库（此函数仅下载备份文件，实际恢复需要手动进行或扩展此函数）
        
        Args:
            backup_path: 备份在存储服务中的路径
            local_path: 本地保存路径，如果为None则使用远程文件名
            remove_local: 恢复完成后是否删除本地文件
            
        Returns:
            bool: 下载是否成功
        """
        try:
            # 连接存储服务
            if not self.storage_manager.is_connected():
                self.storage_manager.connect()
            
            # 如果未指定本地路径，则生成一个
            if local_path is None:
                local_path = os.path.join(self.backup_dir, os.path.basename(backup_path))
            
            # 下载备份文件
            downloaded_path = self.storage_manager.download_file(
                remote_path=backup_path,
                local_path=local_path
            )
            
            self.logger.info(f"已下载备份文件: {downloaded_path}")
            self.logger.warning("注意：需要手动执行恢复操作或扩展此函数实现自动恢复")
            
            # 如果需要，删除本地文件（通常不建议）
            if remove_local and os.path.exists(downloaded_path):
                os.remove(downloaded_path)
                self.logger.info(f"已删除本地备份文件: {downloaded_path}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"下载备份文件失败: {str(e)}")
            raise
    
    def list_backups(self, prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        列出存储服务中的备份文件
        
        Args:
            prefix: 文件前缀过滤器
            
        Returns:
            List[Dict[str, Any]]: 包含备份信息的字典列表
        """
        try:
            # 连接存储服务
            if not self.storage_manager.is_connected():
                self.storage_manager.connect()
            
            # 列出备份文件
            files = self.storage_manager.list_files(prefix=prefix)
            
            # 格式化结果
            result = []
            for file_path, file_size in files:
                result.append({
                    'path': file_path,
                    'size': file_size,
                    'size_human': self._format_size(file_size),
                    'filename': os.path.basename(file_path)
                })
            
            return result
            
        except Exception as e:
            self.logger.error(f"列出备份文件失败: {str(e)}")
            raise
    
    def delete_backup(self, backup_path: str) -> bool:
        """
        删除存储服务中的备份文件
        
        Args:
            backup_path: 备份在存储服务中的路径
            
        Returns:
            bool: 删除是否成功
        """
        try:
            # 连接存储服务
            if not self.storage_manager.is_connected():
                self.storage_manager.connect()
            
            # 删除备份文件
            result = self.storage_manager.delete_file(backup_path)
            
            if result:
                self.logger.info(f"已删除备份文件: {backup_path}")
            else:
                self.logger.warning(f"删除备份文件失败: {backup_path}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"删除备份文件时发生错误: {str(e)}")
            raise
    
    def clean_old_backups(self, days: int = 30, prefix: Optional[str] = None) -> int:
        """
        清理旧的备份文件（基于文件名中的时间戳）
        
        Args:
            days: 保留最近多少天的备份
            prefix: 文件前缀过滤器
            
        Returns:
            int: 已删除的文件数量
        """
        try:
            # 连接存储服务
            if not self.storage_manager.is_connected():
                self.storage_manager.connect()
            
            # 列出备份文件
            files = self.storage_manager.list_files(prefix=prefix)
            
            # 计算截止日期
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
            
            # 删除旧文件
            deleted_count = 0
            for file_path, _ in files:
                # 尝试从文件名中提取时间戳
                try:
                    # 假设文件名格式为 backup_YYYYMMDD_HHMMSS.sql 或类似格式
                    filename = os.path.basename(file_path)
                    # 提取日期部分
                    date_part = filename.split("_")[1]  # YYYYMMDD
                    time_part = filename.split("_")[2].split(".")[0]  # HHMMSS
                    
                    # 解析日期
                    file_date = datetime.datetime.strptime(f"{date_part}_{time_part}", "%Y%m%d_%H%M%S")
                    
                    # 如果文件日期早于截止日期，则删除
                    if file_date < cutoff_date:
                        if self.storage_manager.delete_file(file_path):
                            self.logger.info(f"已删除旧备份文件: {file_path}")
                            deleted_count += 1
                
                except (IndexError, ValueError):
                    # 如果文件名格式不匹配，则跳过
                    self.logger.warning(f"无法从文件名中提取日期: {file_path}")
                    continue
            
            self.logger.info(f"已清理 {deleted_count} 个旧备份文件")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"清理旧备份文件时发生错误: {str(e)}")
            raise
    
    def cleanup(self) -> None:
        """清理临时目录"""
        try:
            if os.path.exists(self.backup_dir):
                shutil.rmtree(self.backup_dir)
                os.makedirs(self.backup_dir, exist_ok=True)
                self.logger.info(f"已清理临时目录: {self.backup_dir}")
        except Exception as e:
            self.logger.error(f"清理临时目录时发生错误: {str(e)}")
            raise
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """将字节大小格式化为人类可读的形式"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"


