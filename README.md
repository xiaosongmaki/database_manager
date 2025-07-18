# Database Backup System

A professional solution for managing MySQL database backups with multiple cloud storage integrations.

## Features

- **Automated MySQL Database Backup**: Creates complete database dumps with customizable options
- **Multiple Storage Options**: 
  - **MinIO Storage Integration**: Securely stores backups in scalable object storage
  - **Google Drive Integration**: Store backups in Google Drive with OAuth2 or Service Account authentication
- **Backup Management**: List, restore, and delete backups with ease
- **Retention Policy**: Automatically clean up old backups based on configurable retention periods
- **Configurable**: Environment-based configuration for all database and storage parameters

## Architecture

The system consists of three main components:

1. **DatabaseManager**: Handles MySQL database connections and export operations
2. **StorageManager**: Abstract base class for storage operations with multiple implementations:
   - **MinioManager**: MinIO object storage operations (upload, download, list, delete)
   - **GoogleDriveManager**: Google Drive storage operations with OAuth2/Service Account support
3. **DatabaseBackup**: Core class that orchestrates the backup process

## Requirements

- Python 3.12+
- MySQL Server
- Storage Backend (choose one or both):
  - MinIO Server
  - Google Drive API access
- Required Python packages (see `pyproject.toml`)

## Installation

1. Clone the repository
2. Create and activate a virtual environment:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```
   pip install -e .
   ```

## Configuration

Create a `.env` file in the project root with the following variables:

```
# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=your_database

# MinIO Configuration
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_SECURE=False
MINIO_BUCKET=database-backups

# Google Drive Configuration (Optional)
GOOGLE_DRIVE_CREDENTIALS_PATH=credentials.json
GOOGLE_DRIVE_TOKEN_PATH=token.pickle
GOOGLE_DRIVE_FOLDER_ID=your_folder_id  # Optional: specific folder ID
GOOGLE_DRIVE_SERVICE_ACCOUNT_PATH=service_account.json  # Optional: for server apps
```

## Usage

### Basic Backup with MinIO

```python
from src.database_manager import MySQLManager
from src.minio_manager import MinioManager
from src.main import DatabaseBackup

# Initialize managers
db_manager = MySQLManager()
storage_manager = MinioManager()

# Create backup instance
backup = DatabaseBackup(db_manager, storage_manager)

# Create and upload backup
backup_path = backup.create_backup()
print(f"Backup created at: {backup_path}")
```

### Basic Backup with Google Drive

```python
from src.database_manager import MySQLManager
from src.google_drive_manager import GoogleDriveManager
from src.main import DatabaseBackup

# Initialize managers
db_manager = MySQLManager()
storage_manager = GoogleDriveManager(
    credentials_path='credentials.json',
    token_path='token.pickle',
    folder_id='your_folder_id'  # Optional
)

# Create backup instance
backup = DatabaseBackup(db_manager, storage_manager)

# Create and upload backup
backup_path = backup.create_backup()
print(f"Backup created at: {backup_path}")
```

### List Backups

```python
backups = backup.list_backups()
for b in backups:
    print(f"{b['filename']} - Size: {b['size_human']}")
```

### Restore Backup

```python
backup.restore_backup("backup_20230101_120000.sql")
```

### Clean Old Backups

```python
# Keep only backups from the last 7 days
deleted_count = backup.clean_old_backups(days=7)
print(f"Deleted {deleted_count} old backups")
```

## Google Drive Setup

### Option 1: OAuth2 Authentication (Recommended for Desktop Apps)

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Google Drive API:
   - Navigate to "APIs & Services" > "Library"
   - Search for "Google Drive API" and enable it
4. Create OAuth2 credentials:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "OAuth client ID"
   - Choose "Desktop application"
   - Download the credentials file and save as `credentials.json`

### Option 2: Service Account (Recommended for Server Apps)

1. In Google Cloud Console, go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "Service account"
3. Fill in the details and create the service account
4. Generate a JSON key for the service account
5. Save the key file and set `GOOGLE_DRIVE_SERVICE_ACCOUNT_PATH` in your environment
6. Share your Google Drive folder with the service account email

### Google Drive Manager Features

- **OAuth2 and Service Account Support**: Choose the authentication method that fits your use case
- **Folder Organization**: Upload files to specific folders by setting `folder_id`
- **File Management**: Upload, download, delete, and list files
- **Automatic Content Type Detection**: Automatically detects file types for proper handling
- **Progress Tracking**: Built-in logging for upload/download progress

### Example: Using Service Account

```python
from src.google_drive_manager import GoogleDriveManager

# Initialize with service account
storage_manager = GoogleDriveManager(
    service_account_path='path/to/service_account.json',
    folder_id='your_google_drive_folder_id'
)

# Connect and test
storage_manager.connect()
print("Connected:", storage_manager.is_connected())

# List files
files = storage_manager.list_files()
for filename, size in files:
    print(f"{filename}: {size} bytes")
```
