"""Main Cloud Function for data ingestion pipeline."""

import pandas as pd
import os
from typing import Dict, Optional

from utils import (
    bq_client, log_error, download_file, load_to_bigquery,
    update_metadata, get_max_id
)
from schemas import SCHEMAS
from validation import validate_excel_file, validate_relationships
from secrets import get_config

def main(request=None):
    """Main function for Cloud Function."""
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
                
                # Validate Excel file
                is_valid, errors = validate_excel_file(df, table_name)
                if not is_valid:
                    log_error(table_name, f"Validation errors: {'; '.join(errors)}")
                    continue
                    
                # Rename columns for relationships
                if table_name == 'sales':
                    df = df.rename(columns={'customer': 'customer_id', 'product': 'product_id'})
                elif table_name == 'support_tickets':
                    df = df.rename(columns={'customer': 'customer_id', 'product': 'product_id'})
                    
                dataframes[table_name] = df
                os.remove(local_file)
            else:
                continue

        if not dataframes:
            log_error('all', 'No files successfully downloaded and validated')
            return 'No files processed', 200

        # Validate relationships
        customers = dataframes.get('customers', pd.DataFrame())
        products = dataframes.get('products', pd.DataFrame())
        valid_dfs = {}
        invalid_dfs = {}

        for table in ['sales', 'support_tickets']:
            if table not in dataframes:
                continue
                
            valid_df, invalid_df = validate_relationships(
                dataframes[table], table, customers, products
            )
            valid_dfs[table] = valid_df
            invalid_dfs[table] = invalid_df

        # Load data
        for table in ['customers', 'products']:
            if table in dataframes:
                load_to_bigquery(
                    dataframes[table], 
                    f'raw_{table}', 
                    SCHEMAS[table], 
                    'WRITE_TRUNCATE',
                    dataset
                )
                update_metadata(f'raw_{table}', dataset=dataset)

        for table in ['sales', 'support_tickets']:
            if table not in valid_dfs:
                continue
                
            valid_df = valid_dfs[table]
            invalid_df = invalid_dfs.get(table, pd.DataFrame())

            if not valid_df.empty:
                if table == 'sales':
                    max_sale_id = get_max_id('raw_sales', 'sale_id', dataset)
                    valid_df = valid_df[valid_df['sale_id'] > max_sale_id]
                    if not valid_df.empty:
                        load_to_bigquery(valid_df, f'raw_{table}', SCHEMAS[table], dataset=dataset)
                        update_metadata(f'raw_{table}', valid_df['sale_id'].max(), dataset)
                else:  # support_tickets
                    load_to_bigquery(valid_df, f'raw_{table}', SCHEMAS[table], dataset=dataset)
                    update_metadata(f'raw_{table}', valid_df['ticket_id'].max(), dataset)

            if not invalid_df.empty:
                load_to_bigquery(
                    invalid_df, 
                    f'invalid_{table}', 
                    SCHEMAS[f'invalid_{table}'],
                    dataset=dataset
                )

        return 'Ingestion complete', 200

    except Exception as e:
        log_error('main', f'Pipeline failed: {str(e)}')
        return f'Error: {str(e)}', 500

if __name__ == '__main__':
    main()