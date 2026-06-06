# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Overview
# MAGIC %md
# MAGIC ## Google Drive to Unity Catalog Ingestion Pipeline
# MAGIC
# MAGIC **Purpose**: This notebook implements an incremental data ingestion pipeline that downloads resume files (PDF and DOCX) from Google Drive to Databricks Unity Catalog volumes.
# MAGIC
# MAGIC ### Key Features
# MAGIC * **Incremental Loading**: Only downloads new or modified files based on Drive modification timestamps
# MAGIC * **Parallel Processing**: Multi-threaded downloads (configurable workers) for optimal performance
# MAGIC * **Folder Structure Preservation**: Maintains original Google Drive folder hierarchy in UC volumes
# MAGIC * **Metadata Tracking**: Delta table tracks file processing history, status, and timestamps
# MAGIC * **Job Orchestration**: Sets task values for downstream workflow control
# MAGIC
# MAGIC ### Configuration Parameters
# MAGIC * `folder_id`: Google Drive folder ID to sync from
# MAGIC * `catalog_name`: Unity Catalog name for destination tables/volumes
# MAGIC * `env`: Environment identifier (dev/prod) for service account credentials
# MAGIC
# MAGIC ### Processing Flow
# MAGIC 1. Authenticate with Google Drive using service account
# MAGIC 2. List all PDF/DOCX files from source folder recursively
# MAGIC 3. Compare with metadata table to identify new/modified files
# MAGIC 4. Download files in parallel to `/Volumes/{catalog}/staging/source_files/`
# MAGIC 5. Update metadata table with processing status
# MAGIC 6. Set job task value based on whether new files were found

# COMMAND ----------

# DBTITLE 1,Install Google Drive API Dependencies
# MAGIC %pip install google-api-python-client google-auth-oauthlib google-auth-httplib2

# COMMAND ----------

# DBTITLE 1,Configure Notebook Parameters
dbutils.widgets.text("folder_id","")
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("env","")
catalog_name=dbutils.widgets.get("catalog_name")
folder_id=dbutils.widgets.get("folder_id")
env=dbutils.widgets.get("env")

# COMMAND ----------

# DBTITLE 1,Set Service Account File Path
SERVICE_ACCOUNT_FILE = f"/Volumes/{catalog_name}/staging/metadata/resumeusecase_{env}.json"

# COMMAND ----------

# DBTITLE 1,Create Source Files Volume
# MAGIC %sql
# MAGIC create volume if not exists resume_use_case.staging.source_files

# COMMAND ----------

# DBTITLE 1,Create Metadata Volume
# MAGIC %sql
# MAGIC create volume if not exists resume_use_case.staging.metadata

# COMMAND ----------

# DBTITLE 1,Authenticate with Google Drive
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build('drive', 'v3', credentials=credentials)

# COMMAND ----------

# DBTITLE 1,Create File Processing Metadata Table
# Create metadata table to track processed files
metadata_table = f"{catalog_name}.staging.file_processing_metadata"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metadata_table} (
    file_id STRING,
    file_name STRING,
    folder_path STRING,
    drive_modified_time TIMESTAMP,
    downloaded_timestamp TIMESTAMP,
    file_size_bytes BIGINT,
    status STRING
)
USING DELTA
""")

print(f"✅ Metadata table ready: {metadata_table}")

# COMMAND ----------

# DBTITLE 1,Get File Metadata from Drive
def get_file_modified_time(file_id):
    """Get the last modified time of a file from Google Drive"""
    try:
        file_metadata = service.files().get(
            fileId=file_id,
            fields='modifiedTime, size'
        ).execute()
        
        return {
            'modifiedTime': file_metadata.get('modifiedTime'),
            'size': file_metadata.get('size', 0)
        }
    except Exception as e:
        print(f"❌ Error getting metadata for file {file_id}: {str(e)}")
        return None

# COMMAND ----------

# DBTITLE 1,Check If File Needs Download
from datetime import datetime, timezone
import threading

# Thread lock for metadata reads (if not already defined)
if 'metadata_lock' not in dir():
    metadata_lock = threading.Lock()

def needs_download(file_id, drive_modified_time):
    """
    Check if file needs to be downloaded by comparing with metadata table
    Returns True if file is new or has been modified
    Thread-safe implementation
    """
    try:
        # Thread-safe query to metadata table
        with metadata_lock:
            existing_record = spark.sql(f"""
                SELECT drive_modified_time, status
                FROM {metadata_table}
                WHERE file_id = '{file_id}'
                ORDER BY downloaded_timestamp DESC
                LIMIT 1
            """).collect()
        
        if not existing_record:
            # File not in metadata table - needs download
            return True
        
        # Get existing timestamp and make it timezone-aware if needed
        existing_modified = existing_record[0]['drive_modified_time']
        if existing_modified.tzinfo is None:
            # Make timezone-naive timestamp aware (assume UTC)
            existing_modified = existing_modified.replace(tzinfo=timezone.utc)
        
        # Parse Drive timestamp as timezone-aware
        drive_time = datetime.fromisoformat(drive_modified_time.replace('Z', '+00:00'))
        
        if drive_time > existing_modified:
            print(f"🔄 File modified on Drive: {file_id}")
            return True
        
        return False
        
    except Exception as e:
        print(f"⚠️ Error checking metadata for {file_id}: {str(e)}")
        # If there's an error, download to be safe
        return True

# COMMAND ----------

# DBTITLE 1,Update Metadata After Processing
import threading

# Thread lock for metadata updates (if not already defined)
if 'metadata_lock' not in dir():
    metadata_lock = threading.Lock()

def update_metadata(file_id, file_name, folder_path, drive_modified_time, file_size, status='SUCCESS'):
    """
    Insert or update metadata record after file processing
    Thread-safe implementation using lock
    """
    try:
        from datetime import datetime
        from pyspark.sql import Row
        
        # Create DataFrame with new record
        new_record = spark.createDataFrame([
            Row(
                file_id=file_id,
                file_name=file_name,
                folder_path=folder_path,
                drive_modified_time=datetime.fromisoformat(drive_modified_time.replace('Z', '+00:00')),
                downloaded_timestamp=datetime.now(),
                file_size_bytes=int(file_size),
                status=status
            )
        ])
        
        # Use lock to ensure thread-safe writes to Delta table
        with metadata_lock:
            new_record.write.mode('append').saveAsTable(metadata_table)
        
    except Exception as e:
        print(f"⚠️ Error updating metadata for {file_id}: {str(e)}")

# COMMAND ----------

# DBTITLE 1,List Files Recursively from Drive
def list_files_with_path(folder_id, current_path=""):
    query = f"'{folder_id}' in parents and trashed=false"

    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType, modifiedTime, size)"  # ✨ Added modifiedTime and size
    ).execute()

    items = results.get('files', [])
    all_files = []

    for item in items:
        item_name = item['name']
        item_id = item['id']
        mime_type = item['mimeType']

        if mime_type == 'application/vnd.google-apps.folder':
            # 📂 Go inside folder
            new_path = f"{current_path}/{item_name}" if current_path else item_name

            all_files.extend(
                list_files_with_path(item_id, new_path)
            )

        else:
            # 📄 File with full path and metadata
            all_files.append({
                "id": item_id,
                "name": item_name,
                "path": current_path,
                "modifiedTime": item.get('modifiedTime'),  # ✨ Drive modification time
                "size": item.get('size', 0)  # ✨ File size
            })

    return all_files

# COMMAND ----------

# DBTITLE 1,Download File with Folder Structure
from googleapiclient.http import MediaIoBaseDownload
import os

VOLUME_BASE_PATH = f"/Volumes/{catalog_name}/staging/source_files/"
VOLUME_BASE_URI  = f"/Volumes/{catalog_name}/staging/source_files/"


def download_with_structure(file_id, file_name, folder_path, drive_modified_time, file_size):
    """
    Download file only if it's new or modified (incremental load)
    """
    try:
        # ✨ Check if file needs download (incremental logic)
        if not needs_download(file_id, drive_modified_time):
            print(f"⏭️ Skipped (up-to-date): {folder_path}/{file_name}")
            return
        
        # 📂 Create folder path in UC Volume
        full_folder_path = os.path.join(VOLUME_BASE_PATH, folder_path)

        if folder_path:
            dbutils.fs.mkdirs(
                VOLUME_BASE_URI + folder_path
            )

        full_file_path = os.path.join(full_folder_path, file_name)

        request = service.files().get_media(fileId=file_id)

        # ✅ Stream directly to correct folder
        with open(full_file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)

            done = False
            while not done:
                status, done = downloader.next_chunk()

        print(f"✅ Downloaded: {folder_path}/{file_name}")
        
        # ✨ Update metadata table after successful download
        update_metadata(file_id, file_name, folder_path, drive_modified_time, file_size, 'SUCCESS')

    except Exception as e:
        print(f"❌ Failed: {file_name} | Error: {str(e)}")
        # Log failure in metadata
        update_metadata(file_id, file_name, folder_path, drive_modified_time, file_size, f'FAILED: {str(e)[:200]}')

# COMMAND ----------

# DBTITLE 1,Process Files in Parallel
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Thread lock for metadata updates
metadata_lock = threading.Lock()

def process_single_file(file):
    """
    Process a single file - designed for parallel execution
    """
    file_name = file['name']
    file_id = file['id']
    folder_path = file['path']
    modified_time = file.get('modifiedTime')
    file_size = file.get('size', 0)

    # ✅ Filter only PDF & DOCX
    if not file_name.lower().endswith((".pdf", ".docx")):
        return None
    
    try:
        # Check if needs download
        if needs_download(file_id, modified_time):
            download_with_structure(file_id, file_name, folder_path, modified_time, file_size)
            return {'status': 'downloaded', 'file': file_name}
        else:
            print(f"⏭️ Skipped (up-to-date): {folder_path}/{file_name}")
            return {'status': 'skipped', 'file': file_name}
    except Exception as e:
        print(f"❌ Error processing {file_name}: {str(e)}")
        return {'status': 'error', 'file': file_name, 'error': str(e)}

def process_drive_with_structure(folder_id, max_workers=10):
    """
    Process Google Drive folder with multi-threading for parallel downloads
    Only downloads new or modified files
    
    Args:
        folder_id: Google Drive folder ID
        max_workers: Number of parallel threads (default: 10)
    """
    print(f"🔄 Starting incremental load with {max_workers} parallel threads...")
    
    all_files = list_files_with_path(folder_id)
    
    # Filter files first
    files_to_process = [
        file for file in all_files 
        if file['name'].lower().endswith((".pdf", ".docx"))
    ]
    
    total_files = len(files_to_process)
    print(f"📁 Found {total_files} files to process")
    
    downloaded_files = 0
    skipped_files = 0
    error_files = 0
    
    # Process files in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_single_file, file): file for file in files_to_process}
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_file):
            result = future.result()
            if result:
                if result['status'] == 'downloaded':
                    downloaded_files += 1
                elif result['status'] == 'skipped':
                    skipped_files += 1
                elif result['status'] == 'error':
                    error_files += 1
    return downloaded_files
    print(f"\n📊 Summary:")
    print(f"   Total files processed: {total_files}")
    print(f"   ✅ Downloaded/Updated: {downloaded_files}")
    print(f"   ⏭️ Skipped (up-to-date): {skipped_files}")
    print(f"   ❌ Errors: {error_files}")

# COMMAND ----------

# DBTITLE 1,Execute Parallel Download
# Run with 10 parallel threads (adjust max_workers as needed)
# max_workers=10 is good for most cases, increase for faster processing
downloaded_files=process_drive_with_structure(folder_id, max_workers=10)

# COMMAND ----------

# DBTITLE 1,Display Downloaded File Count
print(downloaded_files)

# COMMAND ----------

# DBTITLE 1,Set Job Task Value
if (downloaded_files==0):
    print("No new files found. Exiting.")
    dbutils.jobs.taskValues.set(
    key="is_process",
    value="0"
)
else:
    dbutils.jobs.taskValues.set(
    key="is_process",
    value="1"
)

