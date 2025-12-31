# main.py - Cloud Function for Student Data Pipeline

import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import re
import io
import json
import uuid
from datetime import datetime

# ============================================================

PROJECT_ID = "student-data-pipeline"  # Your project ID
RAW_BUCKET = "student-pipeline-raw-incoming"  # Your raw bucket name
STAGING_BUCKET = "student-pipeline-staging-clean"  # Your staging bucket
ARCHIVE_BUCKET = "student-pipeline-archive"  # Your archive bucket
DATASET_ID = "student_data"  # Your BigQuery dataset
STUDENTS_TABLE = "students"  # Your students table
FILE_LOG_TABLE = "file_load_log"  # Your file log table

# ============================================================
# DATA CLEANING FUNCTIONS
# ============================================================

def clean_text(text):
    """
    Clean text by removing special characters.
    Handles: #, !!, @, $, %, ^, &, *, etc.
    Same logic as your Azure Function.
    """
    if pd.isna(text) or text is None or text == "":
        return ""
    text_str = str(text)
    if text_str.strip().upper() == "NULL":
        return ""
    # Remove special characters, keep alphanumeric, spaces, comma, period, hyphen
    cleaned = re.sub(r'[^a-zA-Z0-9\s,.-]', '', text_str)
    # Replace underscores with spaces
    cleaned = cleaned.replace('_', ' ')
    # Remove extra whitespace
    cleaned = ' '.join(cleaned.split())
    return cleaned.strip()


def clean_phone(phone):
    """
    Clean phone numbers to extract last 10 digits.
    Handles scientific notation like 9.87654321E9
    Same logic as your Azure Function.
    """
    if pd.isna(phone):
        return ""
    s_phone = str(phone)
    # Handle scientific notation
    if 'E' in s_phone or 'e' in s_phone:
        try:
            s_phone = str(int(float(phone)))
        except:
            pass
    # Remove all non-digits
    digits = re.sub(r'\D', '', s_phone)
    # Return last 10 digits
    return digits[-10:] if len(digits) >= 10 else digits


def clean_date(date_val):
    """
    Standardize date format to YYYY-MM-DD.
    Same logic as your Azure Function.
    """
    if pd.isna(date_val):
        return None
    try:
        return pd.to_datetime(date_val).strftime('%Y-%m-%d')
    except:
        return None


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_storage_client():
    """Get GCS client."""
    return storage.Client()


def get_bq_client():
    """Get BigQuery client."""
    return bigquery.Client()


def get_file_metadata(bucket_name, file_name):
    """Get file metadata from GCS (like ADF Get Metadata activity)."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.reload()
    
    return {
        'name': blob.name,
        'last_modified': blob.updated,
        'size': blob.size,
        'full_path': f"gs://{bucket_name}/{blob.name}"
    }


def check_file_in_log(file_path):
    """
    Check if file exists in FileLoadLog (like ADF Lookup activity).
    Returns the last processed timestamp if found.
    """
    client = get_bq_client()
    
    query = f"""
        SELECT 
            file_path,
            last_modified_timestamp,
            load_status
        FROM `{PROJECT_ID}.{DATASET_ID}.{FILE_LOG_TABLE}`
        WHERE file_path = @file_path
        ORDER BY created_at DESC
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_path", "STRING", file_path)
        ]
    )
    
    results = list(client.query(query, job_config=job_config).result())
    
    if results:
        row = results[0]
        return {
            'exists': True,
            'last_modified_timestamp': row.last_modified_timestamp,
            'load_status': row.load_status
        }
    return {'exists': False}


def should_process_file(file_metadata, log_entry):
    """
    Determine if file should be processed (like ADF If Condition).
    Returns: (should_process: bool, reason: str)
    """
    file_last_modified = file_metadata['last_modified']
    
    # First load - file not in log
    if not log_entry['exists']:
        return True, "FIRST_LOAD"
    
    log_last_modified = log_entry['last_modified_timestamp']
    
    # Compare timestamps
    if file_last_modified > log_last_modified:
        return True, "FILE_MODIFIED"
    else:
        return False, "NOT_MODIFIED"


def read_file_from_gcs(bucket_name, file_name):
    """Read Excel or CSV file from GCS."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    content = blob.download_as_bytes()
    
    if file_name.endswith('.xlsx') or file_name.endswith('.xls'):
        return pd.read_excel(io.BytesIO(content))
    elif file_name.endswith('.csv'):
        return pd.read_csv(io.BytesIO(content))
    else:
        raise ValueError(f"Unsupported file type: {file_name}")


def write_csv_to_gcs(df, bucket_name, file_name):
    """Write DataFrame as CSV to GCS."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    csv_content = df.to_csv(index=False)
    blob.upload_from_string(csv_content, content_type='text/csv')
    
    return f"gs://{bucket_name}/{file_name}"


def copy_to_archive(source_bucket, source_file, archive_bucket):
    """Copy file to archive bucket (like ADF Copy to Archive)."""
    client = get_storage_client()
    source_bucket_obj = client.bucket(source_bucket)
    archive_bucket_obj = client.bucket(archive_bucket)
    
    source_blob = source_bucket_obj.blob(source_file)
    
    # Create archive path with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    original_filename = source_file.split('/')[-1]
    archive_file = f"archived/{timestamp}_{original_filename}"
    
    # Copy to archive
    source_bucket_obj.copy_blob(source_blob, archive_bucket_obj, archive_file)
    
    return f"gs://{archive_bucket}/{archive_file}"


def delete_file(bucket_name, file_name):
    """Delete file from GCS (like ADF Delete activity)."""
    client = get_storage_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.delete()
    return True


def load_to_bigquery(gcs_uri, source_file):
    """Load CSV to BigQuery (like ADF Copy to SQL)."""
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{STUDENTS_TABLE}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=[
            bigquery.SchemaField("StudentID", "INT64"),
            bigquery.SchemaField("StudentName", "STRING"),
            bigquery.SchemaField("Address", "STRING"),
            bigquery.SchemaField("Phone", "STRING"),
            bigquery.SchemaField("Admission_date", "DATE"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for completion
    
    return load_job.output_rows


def insert_file_log(log_data):
    """Insert record into FileLoadLog (like ADF Stored Procedure)."""
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{FILE_LOG_TABLE}"
    
    log_id = str(uuid.uuid4())
    
    rows = [{
        'log_id': log_id,
        'file_name': log_data.get('file_name'),
        'file_path': log_data.get('file_path'),
        'last_modified_timestamp': log_data.get('last_modified_timestamp').isoformat(),
        'file_size_bytes': log_data.get('file_size_bytes'),
        'rows_processed': log_data.get('rows_processed', 0),
        'load_status': log_data.get('load_status'),
        'staging_file_path': log_data.get('staging_file_path'),
        'archive_file_path': log_data.get('archive_file_path'),
        'error_message': log_data.get('error_message'),
        'processed_by': 'CLOUD_FUNCTION'
    }]
    
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise Exception(f"Failed to insert log: {errors}")
    
    return log_id


# ============================================================
# MAIN PIPELINE FUNCTION
# ============================================================

def process_file(file_name, bucket_name):
    """
    Main pipeline function - mirrors your Azure ADF workflow:
    
    1. Get Metadata
    2. Lookup FileLoadLog
    3. If Condition (compare timestamps)
    4. TRUE: Clean → Staging → BigQuery → Archive → Delete → Log → Notify
    5. FALSE: Log Skip → Notify
    """
    
    print(f"\n{'='*60}")
    print(f"STARTING PIPELINE FOR: {file_name}")
    print(f"{'='*60}\n")
    
    result = {
        'status': None,
        'file_name': file_name,
        'message': None,
        'details': {}
    }
    
    try:
        # =========================================
        # STEP 1: GET METADATA 
        # =========================================
        print("STEP 1: Getting file metadata...")
        metadata = get_file_metadata(bucket_name, file_name)
        print(f"  → Last Modified: {metadata['last_modified']}")
        print(f"  → Size: {metadata['size']} bytes")
        
        # =========================================
        # STEP 2: LOOKUP FILE LOG 
        # =========================================
        print("\nSTEP 2: Checking FileLoadLog...")
        log_entry = check_file_in_log(metadata['full_path'])
        
        if log_entry['exists']:
            print(f"  → Found in log")
            print(f"  → Last processed: {log_entry['last_modified_timestamp']}")
        else:
            print(f"  → Not found in log (first time)")
        
        # =========================================
        # STEP 3: IF CONDITION 
        # =========================================
        print("\nSTEP 3: Evaluating If Condition...")
        should_process, reason = should_process_file(metadata, log_entry)
        print(f"  → Should process: {should_process}")
        print(f"  → Reason: {reason}")
        
        # =========================================
        # FALSE PATH: NOT MODIFIED
        # =========================================
        if not should_process:
            print("\n" + "="*60)
            print("FALSE PATH: File not modified, SKIPPING")
            print("="*60)
            
            # Log the skip
            skip_log = {
                'file_name': file_name.split('/')[-1],
                'file_path': metadata['full_path'],
                'last_modified_timestamp': metadata['last_modified'],
                'file_size_bytes': metadata['size'],
                'rows_processed': 0,
                'load_status': 'SKIPPED',
                'error_message': f'Skipped: {reason}'
            }
            insert_file_log(skip_log)
            
            result['status'] = 'SKIPPED'
            result['message'] = f"File not modified since last processing"
            result['details']['reason'] = reason
            
            print(f"\n✓ Logged skip to FileLoadLog")
            print(f"✓ Pipeline completed (SKIPPED)")
            
            return result
        
        # =========================================
        # TRUE PATH: PROCESS FILE
        # =========================================
        print("\n" + "="*60)
        print("TRUE PATH: Processing file")
        print("="*60)
        
        # =========================================
        # STEP 4: CLEAN DATA 
        # =========================================
        print("\nSTEP 4: Cleaning data...")
        df = read_file_from_gcs(bucket_name, file_name)
        original_rows = len(df)
        print(f"  → Original rows: {original_rows}")
        
        # Apply cleaning (same as your Azure Function)
        df['StudentName'] = df['StudentName'].apply(clean_text)
        df['Address'] = df['Address'].apply(clean_text)
        df['Phone'] = df['Phone'].apply(clean_phone)
        df['Admission_date'] = df['Admission_date'].apply(clean_date)
        df = df.fillna('')
        
        print(f"  → Cleaning complete")
        
        # =========================================
        # STEP 5: WRITE TO STAGING
        # =========================================
        print("\nSTEP 5: Writing to staging bucket...")
        staging_file = f"processed/{file_name.split('/')[-1].rsplit('.', 1)[0]}_cleaned.csv"
        staging_uri = write_csv_to_gcs(df, STAGING_BUCKET, staging_file)
        print(f"  → Staging file: {staging_uri}")
        
        # =========================================
        # STEP 6: LOAD TO BIGQUERY 
        # =========================================
        print("\nSTEP 6: Loading to BigQuery...")
        rows_loaded = load_to_bigquery(staging_uri, file_name)
        print(f"  → Rows loaded: {rows_loaded}")
        
        # =========================================
        # STEP 7: ARCHIVE 
        # =========================================
        print("\nSTEP 7: Archiving original file...")
        archive_path = copy_to_archive(bucket_name, file_name, ARCHIVE_BUCKET)
        print(f"  → Archived to: {archive_path}")
        
        # =========================================
        # STEP 8: DELETE FROM RAW 
        # =========================================
        print("\nSTEP 8: Deleting from raw bucket...")
        delete_file(bucket_name, file_name)
        print(f"  → Deleted: gs://{bucket_name}/{file_name}")
        
        # =========================================
        # STEP 9: CLEANUP STAGING
        # =========================================
        print("\nSTEP 9: Cleaning up staging...")
        delete_file(STAGING_BUCKET, staging_file)
        print(f"  → Deleted staging file")
        
        # =========================================
        # STEP 10: UPDATE FILE LOAD LOG
        # =========================================
        print("\nSTEP 10: Updating FileLoadLog...")
        success_log = {
            'file_name': file_name.split('/')[-1],
            'file_path': metadata['full_path'],
            'last_modified_timestamp': metadata['last_modified'],
            'file_size_bytes': metadata['size'],
            'rows_processed': rows_loaded,
            'load_status': 'SUCCESS',
            'staging_file_path': staging_uri,
            'archive_file_path': archive_path
        }
        log_id = insert_file_log(success_log)
        print(f"  → Log ID: {log_id}")
        
        # =========================================
        # STEP 11: SUCCESS NOTIFICATION
        # =========================================
        print("\n" + "="*60)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        print(f"  File: {file_name}")
        print(f"  Rows Loaded: {rows_loaded}")
        print(f"  Archive: {archive_path}")
        
        result['status'] = 'SUCCESS'
        result['message'] = f"Successfully processed {file_name}"
        result['details'] = {
            'rows_processed': rows_loaded,
            'archive_path': archive_path,
            'log_id': log_id
        }
        
        return result
        
    except Exception as e:
        # =========================================
        # ERROR HANDLING
        # =========================================
        error_msg = str(e)
        print(f"\n❌ ERROR: {error_msg}")
        
        # Log failure
        try:
            failure_log = {
                'file_name': file_name.split('/')[-1],
                'file_path': f"gs://{bucket_name}/{file_name}",
                'last_modified_timestamp': datetime.now(),
                'file_size_bytes': 0,
                'rows_processed': 0,
                'load_status': 'FAILED',
                'error_message': error_msg
            }
            insert_file_log(failure_log)
        except:
            pass
        
        result['status'] = 'FAILED'
        result['message'] = error_msg
        
        raise


# ============================================================
# CLOUD FUNCTION ENTRY POINTS
# ============================================================

@functions_framework.cloud_event
def gcs_trigger(cloud_event):
    """
    Triggered when a file is uploaded to GCS.
    This is like Azure Event Grid trigger.
    """
    data = cloud_event.data
    
    file_name = data["name"]
    bucket_name = data["bucket"]
    
    print(f"Event received: gs://{bucket_name}/{file_name}")
    
    # Only process files in 'incoming' folder
    if not file_name.startswith("incoming/"):
        print(f"Skipping: Not in incoming folder")
        return
    
    # Only process supported file types
    if not any(file_name.lower().endswith(ext) for ext in ['.xlsx', '.xls', '.csv']):
        print(f"Skipping: Unsupported file type")
        return
    
    # Process the file
    result = process_file(file_name, bucket_name)
    print(f"Result: {result['status']}")


@functions_framework.http
def http_trigger(request):
    """
    HTTP trigger for manual invocation.
    Like manually running Azure ADF pipeline.
    """
    request_json = request.get_json(silent=True)
    
    if not request_json or 'fileName' not in request_json:
        return json.dumps({
            "status": "ERROR",
            "message": "Please provide 'fileName' in request body"
        }), 400
    
    file_name = request_json['fileName']
    bucket_name = request_json.get('bucketName', RAW_BUCKET)
    
    try:
        result = process_file(file_name, bucket_name)
        return json.dumps(result), 200
    except Exception as e:
        return json.dumps({
            "status": "FAILED",
            "message": str(e)

        }), 500
