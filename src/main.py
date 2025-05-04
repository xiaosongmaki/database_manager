from prefect import task, flow, get_run_logger
from prefect.blocks.system import Secret
from src.backup import DatabaseBackup
import os
import json

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

@task(name="clean-old-backups-task")
def clean_old_backups_task(db_manager, storage_manager, days=30):
    logger = get_run_logger()
    
    # 创建备份实例
    backup = DatabaseBackup(db_manager, storage_manager)
    
    # 清理旧备份
    try:
        deleted_count = backup.clean_old_backups(days=days)
        logger.info(f"成功清理 {deleted_count} 个超过 {days} 天的旧备份文件")
        return deleted_count
    except Exception as e:
        logger.error(f"清理旧备份过程中发生错误: {str(e)}")
        raise
    finally:
        # 断开连接
        if db_manager.is_connected():
            db_manager.disconnect()


@flow
def database_backup():
    logger = get_run_logger()
    logger.info("开始数据库备份流程")
    
    # 从 Prefect 块中加载配置
    try:
        # 获取配置
        config = Secret.load("backup-config").get()            
        logger.info("成功从 Prefect Secret 块中加载配置")
    except Exception as e:
        logger.error(f"无法从 Prefect Secret 块获取配置: {str(e)}")
        raise
    
    # 示例用法
    from src.database_manager import MySQLManager
    from src.minio_manager import MinioManager
    
    # 创建管理器实例 - 使用配置参数
    db_manager = MySQLManager(
        host=config["MYSQL_HOST"],
        port=int(config["MYSQL_PORT"]),
        user=config["MYSQL_USER"],
        password=config["MYSQL_PASSWORD"],
        database=config["MYSQL_DATABASE"]
    )
    
    storage_manager = MinioManager(
        endpoint=config["MINIO_ENDPOINT"],
        access_key=config["MINIO_ACCESS_KEY"],
        secret_key=config["MINIO_SECRET_KEY"],
        bucket=config["MINIO_BUCKET"],
        secure=config["MINIO_SECURE"] if isinstance(config["MINIO_SECURE"], bool) else config["MINIO_SECURE"].lower() == "true"
    )
    
    # 运行备份任务
    backup_path = create_backup_task(db_manager, storage_manager)
    
    # 清理30天前的旧备份
    deleted_count = clean_old_backups_task(db_manager, storage_manager, days=30)
    
    logger.info(f"备份流程完成，备份路径: {backup_path}，清理了 {deleted_count} 个旧备份")

if __name__ == "__main__":
    main()
