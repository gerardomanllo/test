"""Main Cloud Function for data ingestion pipeline."""

import pandas as pd
import os
import json
import numpy as np
from typing import Dict, Optional, Tuple, Any, Union
from flask import Request

from utils import (
    bq_client, log_error, download_file, load_to_bigquery,
    update_metadata, get_max_id, add_to_log, get_logs, clear_logs
)
from schemas import SCHEMAS
from validation import validate_excel_file, validate_relationships
from config import get_config

def convert_to_python_types(obj: Any) -> Any:
    """
    Convert NumPy/Pandas types to Python native types for JSON serialization.
    
    Args:
        obj: Object to convert
        
    Returns:
        Object with native Python types
    """
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_to_python_types(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_python_types(item) for item in obj]
    return obj

def main(request: Request) -> Tuple[str, int]:
    """
    Main function for Cloud Function.
    
    Args:
        request: Flask request object
        
    Returns:
        Tuple of (response message, HTTP status code)
    """
    # Clear logs from previous runs
    clear_logs()
    
    # Verify request method
    if request.method != 'POST':
        log_error('main', 'Invalid request method')
        return json.dumps({
            'status': 'error',
            'message': 'Method not allowed',
            'logs': get_logs()
        }), 405
        
    try:
        # Get configuration from Secret Manager
        config = get_config()
        dataset = config['dataset']
        bucket = config['bucket']
        files = config['files']
        
        # Download and read files
        dataframes = {}
        for file_name in files:
            local_file = download_file(bucket, file_name)
            if local_file:
                table_name = file_name.replace('.xlsx', '')
                df = pd.read_excel(local_file, engine='openpyxl')
                
                # Rename columns for relationships BEFORE validation
                if table_name == 'sales':
                    df = df.rename(columns={'customer': 'customer_id', 'product': 'product_id'})
                elif table_name == 'support_tickets':
                    df = df.rename(columns={'customer': 'customer_id', 'product': 'product_id'})
                
                # Validate Excel file after renaming
                is_valid, errors = validate_excel_file(df, table_name)
                if not is_valid:
                    log_error(table_name, f"Validation errors: {'; '.join(errors)}")
                    continue
                    
                dataframes[table_name] = df
                os.remove(local_file)
            else:
                log_error('main', f"Failed to process {file_name}")
                continue

        if not dataframes:
            log_error('all', 'No files successfully downloaded and validated')
            return json.dumps({
                'status': 'error',
                'message': 'No files processed',
                'logs': get_logs()
            }), 200

        # Get reference data for relationship validation
        customers = dataframes.get('customers', pd.DataFrame())
        products = dataframes.get('products', pd.DataFrame())

        # Validate relationships and prepare data for loading
        valid_dfs = {}
        invalid_dfs = {}

        # First handle reference tables (customers and products)
        for table in ['customers', 'products']:
            if table in dataframes:
                valid_dfs[table] = dataframes[table]  # These don't need relationship validation

        # Then handle tables with relationships (sales and support_tickets)
        for table in ['sales', 'support_tickets']:
            if table not in dataframes:
                continue
                
            # Only validate if we have both customers and products data
            if customers.empty or products.empty:
                log_error(table, "Cannot validate relationships: missing customers or products data")
                continue
                
            valid_df, invalid_df = validate_relationships(
                dataframes[table], table, customers, products
            )
            valid_dfs[table] = valid_df
            invalid_dfs[table] = invalid_df

        # Load all validated data to BigQuery
        for table in ['customers', 'products', 'support_tickets']:
            if table in valid_dfs:
                load_to_bigquery(
                    valid_dfs[table], 
                    f'raw_{table}', 
                    SCHEMAS[table], 
                    'WRITE_TRUNCATE',
                    dataset
                )
                update_metadata(f'raw_{table}', dataset=dataset)

        # Handle sales separately for incremental loading
        if 'sales' in valid_dfs:
            valid_df = valid_dfs['sales']
            invalid_df = invalid_dfs.get('sales', pd.DataFrame())

            if not valid_df.empty:
                max_id = get_max_id('raw_sales', 'sale_id', dataset)
                new_records = valid_df[valid_df['sale_id'] > max_id]
                if not new_records.empty:
                    load_to_bigquery(new_records, 'raw_sales', SCHEMAS['sales'], dataset=dataset)
                    new_max_id = int(new_records['sale_id'].max())
                    update_metadata('raw_sales', new_max_id, dataset)

            if not invalid_df.empty:
                load_to_bigquery(
                    invalid_df, 
                    'invalid_sales', 
                    SCHEMAS['invalid_sales'],
                    dataset=dataset
                )

        response_data = {
            'status': 'success',
            'message': 'Ingestion complete',
            'tables_processed': list(dataframes.keys()),
            'logs': get_logs()
        }
        return json.dumps(convert_to_python_types(response_data)), 200

    except Exception as e:
        error_msg = f'Pipeline failed: {str(e)}'
        log_error('main', error_msg)
        response_data = {
            'status': 'error',
            'message': error_msg,
            'logs': get_logs()
        }
        return json.dumps(convert_to_python_types(response_data)), 500

if __name__ == '__main__':
    # This is for local testing only
    from flask import Flask, request
    app = Flask(__name__)
    
    @app.route('/', methods=['POST'])
    def handle_request():
        return main(request)
        
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))