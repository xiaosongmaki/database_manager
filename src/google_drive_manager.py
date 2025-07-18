import os
import io
import logging
import pickle
from typing import Optional, List, Tuple
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from googleapiclient.errors import HttpError

from src.config import (
    GOOGLE_DRIVE_CREDENTIALS_PATH,
    GOOGLE_DRIVE_TOKEN_PATH,
    GOOGLE_DRIVE_FOLDER_ID,
    GOOGLE_DRIVE_SERVICE_ACCOUNT_PATH
)

# 导入抽象基类
from src.minio_manager import StorageManager


class GoogleDriveManager(StorageManager):
    """Google Drive存储管理实现"""
    
    # Google Drive API权限范围
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(self, 
                 credentials_path: Optional[str] = GOOGLE_DRIVE_CREDENTIALS_PATH,
                 token_path: Optional[str] = GOOGLE_DRIVE_TOKEN_PATH,
                 folder_id: Optional[str] = GOOGLE_DRIVE_FOLDER_ID,
                 service_account_path: Optional[str] = GOOGLE_DRIVE_SERVICE_ACCOUNT_PATH):
        """
        初始化Google Drive管理器
        
        Args:
            credentials_path: OAuth2客户端凭据文件路径 (credentials.json)
            token_path: 访问令牌存储路径 (token.pickle)
            folder_id: 默认上传文件夹ID，如果为None则上传到根目录
            service_account_path: 服务账户凭据文件路径 (可选，用于服务器端应用)
        """
        self.credentials_path = credentials_path or GOOGLE_DRIVE_CREDENTIALS_PATH
        self.token_path = token_path or GOOGLE_DRIVE_TOKEN_PATH
        self.folder_id = folder_id
        self.service_account_path = service_account_path
        self.service = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> None:
        """连接到Google Drive服务"""
        try:
            creds = None
            
            # 如果指定了服务账户路径，使用服务账户认证
            if self.service_account_path and os.path.exists(self.service_account_path):
                self.logger.info("使用服务账户认证")
                creds = service_account.Credentials.from_service_account_file(
                    self.service_account_path, scopes=self.SCOPES)
            else:
                # 使用OAuth2认证
                self.logger.info("使用OAuth2认证")
                
                # 检查是否存在已保存的令牌
                if os.path.exists(self.token_path):
                    with open(self.token_path, 'rb') as token:
                        creds = pickle.load(token)
                
                # 如果没有有效凭据，进行认证流程
                if not creds or not creds.valid:
                    if creds and creds.expired and creds.refresh_token:
                        # 刷新过期的令牌
                        creds.refresh(Request())
                    else:
                        # 进行新的认证流程
                        if not os.path.exists(self.credentials_path):
                            raise FileNotFoundError(f"凭据文件不存在: {self.credentials_path}")
                        
                        flow = InstalledAppFlow.from_client_secrets_file(
                            self.credentials_path, self.SCOPES)
                        creds = flow.run_local_server(port=0)
                    
                    # 保存凭据以供下次使用
                    with open(self.token_path, 'wb') as token:
                        pickle.dump(creds, token)
            
            # 构建Google Drive API服务
            self.service = build('drive', 'v3', credentials=creds)
            
            # 测试连接
            self.service.about().get(fields="user").execute()
            
            self.logger.info("成功连接到Google Drive服务")
            
        except Exception as err:
            self.logger.error(f"连接Google Drive服务失败: {err}")
            raise
    
    def is_connected(self) -> bool:
        """检查是否已连接到Google Drive服务"""
        if self.service is None:
            return False
        
        try:
            # 尝试获取用户信息来验证连接
            self.service.about().get(fields="user").execute()
            return True
        except:
            return False
    
    def upload_file(self, 
                   local_path: str, 
                   remote_path: Optional[str] = None,
                   content_type: Optional[str] = None) -> str:
        """
        上传文件到Google Drive
        
        Args:
            local_path: 本地文件路径
            remote_path: 远程文件名，如果为None则使用本地文件名
            content_type: 文件内容类型
            
        Returns:
            str: Google Drive文件ID
        """
        if not self.is_connected():
            self.connect()
        
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"本地文件不存在: {local_path}")
        
        # 如果未指定远程文件名，则使用本地文件名
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
            elif extension == '.txt':
                content_type = 'text/plain'
            elif extension == '.json':
                content_type = 'application/json'
            else:
                content_type = 'application/octet-stream'
        
        try:
            # 准备文件元数据
            file_metadata = {
                'name': remote_path
            }
            
            # 如果指定了文件夹ID，设置父文件夹
            if self.folder_id:
                file_metadata['parents'] = [self.folder_id]
            
            # 准备媒体上传
            media = MediaFileUpload(str(local_path), mimetype=content_type)
            
            self.logger.info(f"开始上传文件到Google Drive: {remote_path}")
            
            # 上传文件
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size'
            ).execute()
            
            file_id = file.get('id')
            file_name = file.get('name')
            file_size = file.get('size', 0)
            
            self.logger.info(f"成功上传文件到Google Drive: {file_name} (ID: {file_id}), "
                            f"大小: {file_size} 字节")
            
            return file_id
            
        except HttpError as err:
            self.logger.error(f"上传文件到Google Drive失败: {err}")
            raise
    
    def download_file(self, 
                     remote_path: str, 
                     local_path: Optional[str] = None) -> str:
        """
        从Google Drive下载文件
        
        Args:
            remote_path: Google Drive文件ID或文件名
            local_path: 本地保存路径，如果为None则使用文件名保存到当前目录
            
        Returns:
            str: 本地文件的完整路径
        """
        if not self.is_connected():
            self.connect()
        
        try:
            # 如果remote_path不是文件ID，尝试通过文件名查找
            file_id = remote_path
            if not self._is_file_id(remote_path):
                file_id = self._find_file_by_name(remote_path)
                if not file_id:
                    raise FileNotFoundError(f"在Google Drive中找不到文件: {remote_path}")
            
            # 获取文件信息
            file_info = self.service.files().get(fileId=file_id, fields='name,size').execute()
            file_name = file_info.get('name')
            
            # 如果未指定本地路径，则使用文件名
            if local_path is None:
                local_path = file_name
            
            # 确保本地目录存在
            local_dir = os.path.dirname(local_path)
            if local_dir:
                os.makedirs(local_dir, exist_ok=True)
            
            self.logger.info(f"开始从Google Drive下载文件: {file_name} (ID: {file_id})")
            
            # 下载文件
            request = self.service.files().get_media(fileId=file_id)
            fh = io.FileIO(local_path, mode='wb')
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    self.logger.debug(f"下载进度: {progress}%")
            
            fh.close()
            
            self.logger.info(f"成功从Google Drive下载文件: {file_name} -> {local_path}")
            
            return local_path
            
        except HttpError as err:
            self.logger.error(f"从Google Drive下载文件失败: {err}")
            raise
    
    def delete_file(self, remote_path: str) -> bool:
        """
        从Google Drive删除文件
        
        Args:
            remote_path: Google Drive文件ID或文件名
            
        Returns:
            bool: 删除是否成功
        """
        if not self.is_connected():
            self.connect()
        
        try:
            # 如果remote_path不是文件ID，尝试通过文件名查找
            file_id = remote_path
            if not self._is_file_id(remote_path):
                file_id = self._find_file_by_name(remote_path)
                if not file_id:
                    self.logger.warning(f"在Google Drive中找不到文件: {remote_path}")
                    return False
            
            # 获取文件名用于日志
            try:
                file_info = self.service.files().get(fileId=file_id, fields='name').execute()
                file_name = file_info.get('name', file_id)
            except:
                file_name = file_id
            
            self.logger.info(f"开始从Google Drive删除文件: {file_name} (ID: {file_id})")
            
            # 删除文件
            self.service.files().delete(fileId=file_id).execute()
            
            self.logger.info(f"成功从Google Drive删除文件: {file_name}")
            
            return True
            
        except HttpError as err:
            self.logger.error(f"从Google Drive删除文件失败: {err}")
            return False
    
    def list_files(self, prefix: Optional[str] = None) -> List[Tuple[str, int]]:
        """
        列出Google Drive中的文件
        
        Args:
            prefix: 文件名前缀过滤器
            
        Returns:
            List[Tuple[str, int]]: 包含文件名和大小的元组列表
        """
        if not self.is_connected():
            self.connect()
        
        result = []
        
        try:
            # 构建查询条件
            query = "trashed=false"
            
            # 如果指定了文件夹ID，只列出该文件夹中的文件
            if self.folder_id:
                query += f" and '{self.folder_id}' in parents"
            
            # 如果指定了前缀，添加名称过滤
            if prefix:
                query += f" and name contains '{prefix}'"
            
            # 列出文件
            page_token = None
            while True:
                response = self.service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, size, mimeType)',
                    pageToken=page_token
                ).execute()
                
                files = response.get('files', [])
                
                for file in files:
                    file_name = file.get('name')
                    file_size = int(file.get('size', 0)) if file.get('size') else 0
                    mime_type = file.get('mimeType', '')
                    
                    # 跳过文件夹
                    if mime_type == 'application/vnd.google-apps.folder':
                        continue
                    
                    result.append((file_name, file_size))
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            
            self.logger.info(f"从Google Drive列出了 {len(result)} 个文件")
            
            return result
            
        except HttpError as err:
            self.logger.error(f"列出Google Drive文件失败: {err}")
            raise
    
    def _is_file_id(self, path: str) -> bool:
        """判断给定的字符串是否是Google Drive文件ID"""
        # Google Drive文件ID通常是长度为28-44的字符串，包含字母、数字、下划线和连字符
        return len(path) > 20 and all(c.isalnum() or c in '_-' for c in path)
    
    def _find_file_by_name(self, file_name: str) -> Optional[str]:
        """通过文件名查找文件ID"""
        try:
            query = f"name='{file_name}' and trashed=false"
            
            # 如果指定了文件夹ID，只在该文件夹中搜索
            if self.folder_id:
                query += f" and '{self.folder_id}' in parents"
            
            response = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = response.get('files', [])
            if files:
                return files[0].get('id')
            
            return None
            
        except HttpError:
            return None
    
    def create_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """
        在Google Drive中创建文件夹
        
        Args:
            folder_name: 文件夹名称
            parent_folder_id: 父文件夹ID，如果为None则在根目录创建
            
        Returns:
            str: 创建的文件夹ID
        """
        if not self.is_connected():
            self.connect()
        
        try:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            # 设置父文件夹
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]
            elif self.folder_id:
                file_metadata['parents'] = [self.folder_id]
            
            folder = self.service.files().create(
                body=file_metadata,
                fields='id,name'
            ).execute()
            
            folder_id = folder.get('id')
            self.logger.info(f"成功创建文件夹: {folder_name} (ID: {folder_id})")
            
            return folder_id
            
        except HttpError as err:
            self.logger.error(f"创建文件夹失败: {err}")
            raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # 示例用法
    manager = GoogleDriveManager()
    
    try:
        manager.connect()
        print("连接状态:", manager.is_connected())
        
        # 测试列出所有文件
        files = manager.list_files()
        print("Google Drive文件列表:")
        for f, size in files:
            print(f"{f} ({size} bytes)")
        
        # 测试上传文件
        # test_file = "test_upload.txt"
        # with open(test_file, "w", encoding='utf-8') as f:
        #     f.write("这是一个测试文件，用于验证Google Drive上传功能")
        # 
        # file_id = manager.upload_file(test_file)
        # print(f"文件已成功上传，文件ID: {file_id}")
        # 
        # # 清理测试文件
        # os.remove(test_file)
        
    except Exception as e:
        print(f"操作失败: {e}")

