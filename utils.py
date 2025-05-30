"""Utility functions for the ingestion pipeline."""

import os
import pandas as pd
from google.cloud import bigquery, storage, logging
from typing import Dict, List, Optional, Tuple, Any
import tempfile

# Initialize clients
bq_client = bigquery.Client()
storage_client = storage.Client()
logging_client = logging.Client()

# Create a logger
logger = logging_client.logger('bixlabs-ingestion')

# Global log buffer for collecting logs
log_buffer: List[str] = []

def add_to_log(message: str, severity: str = 'INFO') -> None:
    """
    Add a message to both Cloud Logging and the log buffer.
    Only logs errors by default.
    
    Args:
        message: Message to log
        severity: Log severity (INFO, ERROR, etc.)
    """
    if severity == 'ERROR':
        logger.log_text(message, severity=severity)
        log_buffer.append(f"[{severity}] {message}")

def log_error(component: str, message: str) -> None:
    """
    Log an error message.
    
    Args:
        component: Component where the error occurred
        message: Error message
    """
    error_msg = f"{component}: {message}"
    add_to_log(error_msg, 'ERROR')

def get_logs() -> str:
    """Get all logs as a single string."""
    return '\n'.join(log_buffer)

def clear_logs() -> None:
    """Clear the log buffer."""
    log_buffer.clear()

def download_file(bucket_name: str, file_name: str) -> Optional[str]:
    """
    Download a file from GCS to a temporary location.
    
    Args:
        bucket_name: Name of the GCS bucket
        file_name: Name of the file to download
        
    Returns:
        Path to the downloaded file or None if download failed
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        if not blob.exists():
            log_error('download', f"File {file_name} not found in bucket {bucket_name}")
            return None
            
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
        blob.download_to_filename(temp_file.name)
        return temp_file.name
        
    except Exception as e:
        log_error('download', f"Failed to download {file_name}: {str(e)}")
        return None

def load_to_bigquery(
    df: pd.DataFrame,
    table_name: str,
    schema: List[Dict[str, str]],
    write_disposition: str = 'WRITE_APPEND',
    dataset: str = 'raw_data'
) -> None:
    """
    Load a DataFrame to BigQuery.
    
    Args:
        df: DataFrame to load
        table_name: Name of the target table
        schema: BigQuery schema
        write_disposition: Write disposition for the load job
        dataset: Target dataset
    """
    if df.empty:
        return
        
    try:
        table_id = f"{bq_client.project}.{dataset}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField(**field) for field in schema],
            write_disposition=write_disposition
        )
        
        job = bq_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
    except Exception as e:
        log_error('load', f"Failed to load {table_name}: {str(e)}")

def update_metadata(
    table_name: str,
    max_id: Optional[int] = None,
    dataset: str = 'raw_data'
) -> None:
    """
    Update ingestion metadata for a table.
    
    Args:
        table_name: Name of the table
        max_id: Maximum ID processed (for incremental tables)
        dataset: Dataset containing the table
    """
    try:
        table_id = f"{bq_client.project}.{dataset}.{table_name}"
        table = bq_client.get_table(table_id)
        
        metadata = {
            'last_updated': pd.Timestamp.now().isoformat(),
            'row_count': table.num_rows
        }
        
        if max_id is not None:
            metadata['max_id'] = max_id
            
        table.description = str(metadata)
        bq_client.update_table(table, ['description'])
        
    except Exception as e:
        log_error('metadata', f"Failed to update metadata for {table_name}: {str(e)}")

def get_max_id(table_name: str, id_column: str, dataset: str = 'raw_data') -> int:
    """
    Get the maximum ID from a table.
    
    Args:
        table_name: Name of the table
        id_column: Name of the ID column
        dataset: Dataset containing the table
        
    Returns:
        Maximum ID found, or 0 if no records exist
    """
    try:
        query = f"""
        SELECT MAX({id_column}) as max_id
        FROM `{bq_client.project}.{dataset}.{table_name}`
        """
        query_job = bq_client.query(query)
        result = next(query_job.result())
        return result.max_id or 0
        
    except Exception as e:
        log_error('query', f"Failed to get max_id from {table_name}: {str(e)}")
        return 0 