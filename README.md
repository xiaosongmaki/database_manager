# Database Backup System

A professional solution for managing MySQL database backups with MinIO object storage integration.

## Features

- **Automated MySQL Database Backup**: Creates complete database dumps with customizable options
- **MinIO Storage Integration**: Securely stores backups in scalable object storage
- **Backup Management**: List, restore, and delete backups with ease
- **Retention Policy**: Automatically clean up old backups based on configurable retention periods
- **Configurable**: Environment-based configuration for all database and storage parameters

## Architecture

The system consists of three main components:

1. **DatabaseManager**: Handles MySQL database connections and export operations
2. **StorageManager**: Manages MinIO storage operations (upload, download, list, delete)
3. **DatabaseBackup**: Core class that orchestrates the backup process

## Requirements

- Python 3.12+
- MySQL Server
- MinIO Server
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
```

## Usage

### Basic Backup

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

