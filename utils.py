"""Utility functions for the ingestion pipeline."""

import os
import datetime
from google.cloud import bigquery, storage, logging
from google.api_core import retry
import pandas as pd
from typing import Optional, List

# Initialize clients
bq_client = bigquery.Client()
storage_client = storage.Client()
logging_client = logging.Client()
log = logging_client.logger('bixlabs-ingestion')

# Global log buffer
log_buffer: List[str] = []

def add_to_log(message: str, severity: str = 'INFO') -> None:
    """Add message to both Cloud Logging and the log buffer."""
    timestamp = datetime.datetime.utcnow().isoformat()
    log_message = f"[{timestamp}] {severity}: {message}"
    log_buffer.append(log_message)
    log.log_text(message, severity=severity)

def get_logs() -> str:
    """Get all logs as a single string."""
    return "\n".join(log_buffer)

def clear_logs() -> None:
    """Clear the log buffer."""
    log_buffer.clear()

def log_error(table_name: str, error_message: str) -> None:
    """Log errors to BigQuery and Cloud Logging."""
    add_to_log(f'Error for {table_name}: {error_message}', 'ERROR')
    error_row = {
        'table_name': table_name,
        'error_message': error_message,
        'timestamp': datetime.datetime.utcnow().isoformat()
    }
    try:
        errors_table = bq_client.get_table(f'{bq_client.project}.challenge.ingestion_errors')
        bq_client.insert_rows(errors_table, [error_row])
    except Exception as e:
        add_to_log(f'Failed to write to BigQuery: {str(e)}', 'ERROR')

def download_file(bucket: str, file_name: str, local_path: str = '/tmp') -> Optional[str]:
    """Download file from Cloud Storage."""
    try:
        add_to_log(f'Attempting to download {file_name} from bucket {bucket}')
        
        # Check if bucket exists
        bucket_obj = storage_client.bucket(bucket)
        if not bucket_obj.exists():
            log_error(file_name, f'Bucket {bucket} does not exist')
            return None
            
        # Check if file exists in bucket
        blob = bucket_obj.blob(file_name)
        if not blob.exists():
            log_error(file_name, f'File {file_name} does not exist in bucket {bucket}')
            return None
            
        # Download file
        local_file = os.path.join(local_path, file_name)
        add_to_log(f'Downloading {file_name} to {local_file}')
        blob.download_to_filename(local_file)
        
        # Verify file was downloaded
        if not os.path.exists(local_file):
            log_error(file_name, f'File download failed: {local_file} does not exist')
            return None
            
        add_to_log(f'Successfully downloaded {file_name}')
        return local_file
        
    except Exception as e:
        log_error(file_name, f'Failed to download: {str(e)}')
        return None

@retry.Retry(predicate=retry.if_transient_error, initial=5, maximum=20, multiplier=2)
def load_to_bigquery(df: pd.DataFrame, 
                    table_name: str, 
                    schema: list, 
                    write_disposition: str = 'WRITE_APPEND',
                    dataset: str = 'challenge') -> None:
    """Load DataFrame to BigQuery with retries."""
    table_id = f'{bq_client.project}.{dataset}.{table_name}'
    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(f['name'], f['type'], mode=f['mode']) for f in schema],
        write_disposition=write_disposition
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    add_to_log(f'Loaded {len(df)} rows to {table_name}')

def update_metadata(table_name: str, max_id: Optional[int] = None, dataset: str = 'challenge') -> None:
    """Update ingestion metadata."""
    row = {
        'table_name': table_name,
        'max_id': max_id,
        'last_ingestion_timestamp': datetime.datetime.utcnow().isoformat()
    }
    table = bq_client.get_table(f'{bq_client.project}.{dataset}.ingestion_metadata')
    bq_client.insert_rows(table, [row])

def get_max_id(table_name: str, id_column: str, dataset: str = 'challenge') -> int:
    """Get maximum ID for incremental processing."""
    query = f"""
    SELECT MAX({id_column}) as max_id
    FROM `{bq_client.project}.{dataset}.ingestion_metadata`
    WHERE table_name = '{table_name}'
    """
    result = bq_client.query(query).result()
    for row in result:
        return row.max_id
    return 0 