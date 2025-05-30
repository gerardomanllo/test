"""Input validation functions for the ingestion pipeline."""

import pandas as pd
from typing import Dict, List, Tuple
from .schemas import REQUIRED_COLUMNS

def validate_excel_file(df: pd.DataFrame, table_name: str) -> Tuple[bool, List[str]]:
    """
    Validate an Excel file against required columns and data types.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        
    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []
    
    # Check required columns
    if table_name not in REQUIRED_COLUMNS:
        return False, [f"Unknown table name: {table_name}"]
        
    required_cols = REQUIRED_COLUMNS[table_name]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing required columns: {', '.join(missing_cols)}")
    
    # Check for empty DataFrame
    if df.empty:
        errors.append("DataFrame is empty")
        
    # Check for null values in required columns
    if not df.empty:
        null_cols = df[required_cols].columns[df[required_cols].isnull().any()].tolist()
        if null_cols:
            errors.append(f"Null values found in required columns: {', '.join(null_cols)}")
            
    # Check data types for specific columns
    if table_name == 'sales':
        if 'sale_id' in df.columns and not pd.api.types.is_integer_dtype(df['sale_id']):
            errors.append("sale_id must be integer type")
        if 'quantity' in df.columns and not pd.api.types.is_integer_dtype(df['quantity']):
            errors.append("quantity must be integer type")
            
    elif table_name == 'products':
        if 'price_usd' in df.columns and not pd.api.types.is_float_dtype(df['price_usd']):
            errors.append("price_usd must be float type")
        if 'active' in df.columns and not pd.api.types.is_bool_dtype(df['active']):
            errors.append("active must be boolean type")
            
    return len(errors) == 0, errors

def validate_relationships(df: pd.DataFrame, 
                         table_name: str, 
                         customers_df: pd.DataFrame, 
                         products_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validate relationships between tables and separate valid/invalid records.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table being validated
        customers_df: Customers DataFrame for relationship validation
        products_df: Products DataFrame for relationship validation
        
    Returns:
        Tuple of (valid DataFrame, invalid DataFrame)
    """
    valid_df = df.copy()
    invalid_rows = []
    
    # Validate customer relationships
    if 'customer_id' in df.columns and not customers_df.empty:
        invalid_cust = df[~df['customer_id'].isin(customers_df['customer_id'])]
        if not invalid_cust.empty:
            invalid_cust = invalid_cust.copy()
            invalid_cust['error_reason'] = invalid_cust['customer_id'].apply(
                lambda x: f'Missing customer_id: {x}')
            invalid_cust['ingestion_timestamp'] = pd.Timestamp.utcnow()
            invalid_rows.append(invalid_cust)
            valid_df = valid_df[valid_df['customer_id'].isin(customers_df['customer_id'])]
    
    # Validate product relationships
    if 'product_id' in df.columns and not products_df.empty:
        invalid_prod = valid_df[~valid_df['product_id'].isin(products_df['product_id'])]
        if not invalid_prod.empty:
            invalid_prod = invalid_prod.copy()
            invalid_prod['error_reason'] = invalid_prod['product_id'].apply(
                lambda x: f'Missing product_id: {x}')
            invalid_prod['ingestion_timestamp'] = pd.Timestamp.utcnow()
            invalid_rows.append(invalid_prod)
            valid_df = valid_df[valid_df['product_id'].isin(products_df['product_id'])]
    
    invalid_df = pd.concat(invalid_rows, ignore_index=True) if invalid_rows else pd.DataFrame()
    return valid_df, invalid_df 