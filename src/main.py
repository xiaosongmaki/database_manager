from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from src.backup import DatabaseBackup
import os

@task(name="create-backup-task")
def create_backup_task(db_manager, storage_manager):
    logger = get_run_logger()
    
    # 创建备份实例
    backup = DatabaseBackup(db_manager, storage_manager)
    
    # 创建备份
    try:
        backup_path = backup.create_backup(lock_tables=False)
        logger.info(f"备份已创建: {backup_path}")
        
        # 列出所有备份
        backups = backup.list_backups()
        logger.info(f"找到 {len(backups)} 个备份:")
        for b in backups:
            logger.info(f"  - {b['filename']} ({b['size_human']})")
        
        return backup_path
            
    except Exception as e:
        logger.error(f"备份过程中发生错误: {str(e)}")
        raise
    finally:
        # 断开连接
        db_manager.disconnect()
        
        # 清理临时文件
        backup.cleanup()


@flow
def main():
    logger = get_run_logger()
    logger.info("开始数据库备份流程")
    
    # 从 Prefect 块中加载环境变量
    try:
        # 示例：获取数据库连接信息
        db_host = Secret.load("mysql-host").get()
        db_port = Secret.load("mysql-port").get()
        db_user = Secret.load("mysql-user").get()
        db_password = Secret.load("mysql-password").get()
        db_name = Secret.load("mysql-database").get()
        
        # 存储服务连接信息
        minio_endpoint = Secret.load("minio-endpoint").get()
        minio_access_key = Secret.load("minio-access-key").get()
        minio_secret_key = Secret.load("minio-secret-key").get()
        minio_bucket = Secret.load("minio-bucket").get()
        
        logger.info("成功从 Prefect Secret 块中加载配置")
    except Exception as e:
        logger.error(f"无法从 Prefect Secret 块获取配置: {str(e)}")
        raise
    
    # 示例用法
    from src.database_manager import MySQLManager
    from src.minio_manager import MinioManager
    
    # 创建管理器实例 - 使用配置参数
    db_manager = MySQLManager(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    storage_manager = MinioManager(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        bucket=minio_bucket
    )
    
    # 运行备份任务
    backup_path = create_backup_task(db_manager, storage_manager)
    
    logger.info(f"备份流程完成，备份路径: {backup_path}")

if __name__ == "__main__":
    main()
