"""Input validation functions for the ingestion pipeline."""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from schemas import REQUIRED_COLUMNS
from utils import add_to_log

def validate_excel_file(df: pd.DataFrame, table_name: str) -> Tuple[bool, List[str]]:
    """
    Validate Excel file against schema requirements.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Handle products table specific conversions
    if table_name == 'products':
        # Convert price_usd to float
        if 'price_usd' in df.columns:
            try:
                # First convert to numeric (this handles both string and integer inputs)
                df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
                # Then convert to float
                df['price_usd'] = df['price_usd'].astype(float)
                # Log any rows where conversion failed
                null_prices = df['price_usd'].isnull().sum()
                if null_prices > 0:
                    add_to_log(f"Warning: {null_prices} rows in products have invalid price_usd values", 'WARNING')
            except Exception as e:
                errors.append(f"Failed to convert price_usd to float: {str(e)}")
                return False, errors
                
        # Convert active to boolean
        if 'active' in df.columns:
            try:
                # Convert string TRUE/FALSE to boolean
                df['active'] = df['active'].map({
                    'TRUE': True,
                    'FALSE': False,
                    True: True,
                    False: False,
                    'true': True,
                    'false': False,
                    'True': True,
                    'False': False,
                    '1': True,
                    '0': False,
                    1: True,
                    0: False
                })
                # Log any rows where conversion failed
                null_active = df['active'].isnull().sum()
                if null_active > 0:
                    add_to_log(f"Warning: {null_active} rows in products have invalid active values", 'WARNING')
            except Exception as e:
                errors.append(f"Failed to convert active to boolean: {str(e)}")
                return False, errors
    
    # Validate required columns
    required_columns = {
        'customers': ['customer_id', 'name', 'country', 'industry', 'registration_date'],
        'products': ['product_id', 'description', 'category', 'price_usd', 'active'],
        'sales': ['sale_id', 'customer_id', 'product_id', 'sale_date', 'quantity', 'channel', 'payment_method'],
        'support_tickets': ['ticket_id', 'customer_id', 'product_id', 'status', 'priority', 'opened_at', 'handled_by']
    }
    
    if table_name not in required_columns:
        errors.append(f"Unknown table name: {table_name}")
        return False, errors
        
    missing_columns = set(required_columns[table_name]) - set(df.columns)
    if missing_columns:
        errors.append(f"Missing required columns: {', '.join(missing_columns)}")
        
    # Validate data types after conversion
    if table_name == 'products':
        if 'price_usd' in df.columns and not pd.api.types.is_float_dtype(df['price_usd']):
            errors.append("price_usd must be float type after conversion")
        if 'active' in df.columns and not pd.api.types.is_bool_dtype(df['active']):
            errors.append("active must be boolean type after conversion")
            
    # Validate date columns
    date_columns = {
        'customers': ['registration_date'],
        'sales': ['sale_date'],
        'support_tickets': ['opened_at']
    }
    
    if table_name in date_columns:
        for col in date_columns[table_name]:
            if col in df.columns:
                try:
                    pd.to_datetime(df[col])
                except Exception as e:
                    errors.append(f"{col} must be a valid date: {str(e)}")
                    
    return len(errors) == 0, errors

def validate_relationships(
    df: pd.DataFrame,
    table_name: str,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate relationships between tables.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        customers_df: Customers reference DataFrame
        products_df: Products reference DataFrame
        
    Returns:
        Tuple of (valid DataFrame, invalid DataFrame)
    """
    if customers_df.empty or products_df.empty:
        return pd.DataFrame(), df
        
    # Get valid IDs
    valid_customer_ids = set(customers_df['customer_id'])
    valid_product_ids = set(products_df['product_id'])
    
    # Create masks for valid relationships
    valid_customers = df['customer_id'].isin(valid_customer_ids)
    valid_products = df['product_id'].isin(valid_product_ids)
    
    # Split into valid and invalid
    valid_df = df[valid_customers & valid_products].copy()
    invalid_df = df[~(valid_customers & valid_products)].copy()
    
    # Add validation reason to invalid records
    if not invalid_df.empty:
        invalid_df['validation_reason'] = ''
        invalid_df.loc[~valid_customers, 'validation_reason'] = 'Invalid customer_id'
        invalid_df.loc[~valid_products, 'validation_reason'] = 'Invalid product_id'
        invalid_df.loc[~(valid_customers & valid_products), 'validation_reason'] = 'Invalid customer_id and product_id'
        
    return valid_df, invalid_df 