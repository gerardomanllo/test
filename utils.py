"""Utility functions for the ingestion pipeline."""

import os
import datetime
from google.cloud import bigquery, storage, logging
from google.api_core import retry
import pandas as pd
from typing import Optional

# Initialize clients
bq_client = bigquery.Client()
storage_client = storage.Client()
logging_client = logging.Client()
log = logging_client.logger('bixlabs-ingestion')

def log_error(table_name: str, error_message: str) -> None:
    """Log errors to BigQuery and Cloud Logging."""
    log.log_text(f'Error for {table_name}: {error_message}', severity='ERROR')
    error_row = {
        'table_name': table_name,
        'error_message': error_message,
        'timestamp': datetime.datetime.utcnow().isoformat()
    }
    errors_table = bq_client.get_table(f'{bq_client.project}.challenge.ingestion_errors')
    bq_client.insert_rows(errors_table, [error_row])

def download_file(bucket: str, file_name: str, local_path: str = '/tmp') -> Optional[str]:
    """Download file from Cloud Storage."""
    try:
        bucket = storage_client.bucket(bucket)
        blob = bucket.blob(file_name)
        local_file = os.path.join(local_path, file_name)
        blob.download_to_filename(local_file)
        log.log_text(f'Downloaded {file_name}', severity='INFO')
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
    log.log_text(f'Loaded {len(df)} rows to {table_name}', severity='INFO')

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