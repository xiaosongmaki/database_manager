#!/usr/bin/env python3
"""
Google Drive Manager 使用示例

这个脚本演示了如何使用GoogleDriveManager类来管理Google Drive中的文件。
"""

import os
import sys
import logging
from pathlib import Path

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.google_drive_manager import GoogleDriveManager
from src.database_manager import MySQLManager
from src.backup import DatabaseBackup

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def example_basic_operations():
    """基本操作示例"""
    print("=== Google Drive Manager 基本操作示例 ===")
    
    # 初始化Google Drive管理器
    # 方式1: 使用OAuth2认证（适合桌面应用）
    manager = GoogleDriveManager(
        credentials_path='credentials.json',
        token_path='token.pickle'
    )
    
    # 方式2: 使用服务账户认证（适合服务器应用）
    # manager = GoogleDriveManager(
    #     service_account_path='service_account.json',
    #     folder_id='your_folder_id'
    # )
    
    try:
        # 连接到Google Drive
        print("正在连接到Google Drive...")
        manager.connect()
        print(f"连接状态: {manager.is_connected()}")
        
        # 列出文件
        print("\n--- 列出Google Drive中的文件 ---")
        files = manager.list_files()
        if files:
            for filename, size in files:
                print(f"{filename}: {size} bytes")
        else:
            print("没有找到文件")
        
        # 创建测试文件并上传
        print("\n--- 上传文件测试 ---")
        test_file = "test_google_drive.txt"
        test_content = "这是一个测试文件，用于验证Google Drive上传功能\\n创建时间: " + str(os.path.getmtime(__file__))
        
        with open(test_file, "w", encoding='utf-8') as f:
            f.write(test_content)
        
        # 上传文件
        file_id = manager.upload_file(test_file, "google_drive_test.txt")
        print(f"文件已成功上传，文件ID: {file_id}")
        
        # 再次列出文件，确认上传成功
        print("\n--- 上传后的文件列表 ---")
        files = manager.list_files()
        for filename, size in files:
            print(f"{filename}: {size} bytes")
        
        # 下载文件测试
        print("\n--- 下载文件测试 ---")
        download_path = "downloaded_test.txt"
        manager.download_file("google_drive_test.txt", download_path)
        print(f"文件已下载到: {download_path}")
        
        # 验证下载的文件内容
        if os.path.exists(download_path):
            with open(download_path, "r", encoding='utf-8') as f:
                content = f.read()
                print(f"下载文件内容: {content[:50]}...")
        
        # 清理测试文件
        print("\n--- 清理测试文件 ---")
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"已删除本地文件: {test_file}")
        
        if os.path.exists(download_path):
            os.remove(download_path)
            print(f"已删除下载文件: {download_path}")
        
        # 删除Google Drive中的测试文件
        success = manager.delete_file("google_drive_test.txt")
        if success:
            print("已删除Google Drive中的测试文件")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()

def example_database_backup():
    """数据库备份到Google Drive示例"""
    print("\n=== 数据库备份到Google Drive示例 ===")
    
    # 注意：这个示例需要有效的MySQL配置
    try:
        # 初始化管理器
        db_manager = MySQLManager()
        storage_manager = GoogleDriveManager(
            folder_id='your_backup_folder_id'  # 可选：指定备份文件夹
        )
        
        # 创建备份实例
        backup = DatabaseBackup(db_manager, storage_manager)
        
        # 创建并上传备份
        print("正在创建数据库备份...")
        backup_path = backup.create_backup()
        print(f"备份已创建并上传到Google Drive: {backup_path}")
        
        # 列出所有备份
        print("\n--- 备份文件列表 ---")
        backups = backup.list_backups()
        for b in backups:
            print(f"{b['filename']} - 大小: {b['size_human']} - 创建时间: {b['created_at']}")
        
    except Exception as e:
        print(f"数据库备份示例失败: {e}")
        print("请确保已正确配置MySQL连接参数")

def example_folder_operations():
    """文件夹操作示例"""
    print("\n=== Google Drive 文件夹操作示例 ===")
    
    manager = GoogleDriveManager()
    
    try:
        manager.connect()
        
        # 创建文件夹
        print("正在创建测试文件夹...")
        folder_id = manager.create_folder("测试备份文件夹")
        print(f"文件夹已创建，ID: {folder_id}")
        
        # 在新文件夹中上传文件
        print("正在向新文件夹上传文件...")
        manager.folder_id = folder_id  # 设置默认文件夹
        
        test_file = "folder_test.txt"
        with open(test_file, "w", encoding='utf-8') as f:
            f.write("这是在文件夹中的测试文件")
        
        file_id = manager.upload_file(test_file)
        print(f"文件已上传到文件夹，文件ID: {file_id}")
        
        # 列出文件夹中的文件
        print("文件夹中的文件:")
        files = manager.list_files()
        for filename, size in files:
            print(f"  {filename}: {size} bytes")
        
        # 清理
        os.remove(test_file)
        manager.delete_file(file_id)
        print("已清理测试文件")
        
    except Exception as e:
        print(f"文件夹操作失败: {e}")

def main():
    """主函数"""
    setup_logging()
    
    print("Google Drive Manager 示例程序")
    print("=" * 50)
    
    # 检查凭据文件
    if not os.path.exists('credentials.json') and not os.path.exists('service_account.json'):
        print("错误: 未找到凭据文件!")
        print("请确保以下文件之一存在:")
        print("  - credentials.json (OAuth2认证)")
        print("  - service_account.json (服务账户认证)")
        print("\\n请参考README.md中的Google Drive设置指南")
        return
    
    try:
        # 运行示例
        example_basic_operations()
        # example_database_backup()  # 需要MySQL配置
        # example_folder_operations()
        
    except KeyboardInterrupt:
        print("\\n程序被用户中断")
    except Exception as e:
        print(f"程序执行出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

