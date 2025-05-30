"""Schema definitions for BigQuery tables."""

SCHEMAS = {
    'sales': [
        {'name': 'sale_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'sale_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'channel', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'payment_method', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    'products': [
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'description', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'category', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price_usd', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
        {'name': 'active', 'type': 'BOOLEAN', 'mode': 'REQUIRED'}
    ],
    'customers': [
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'industry', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'}
    ],
    'support_tickets': [
        {'name': 'ticket_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'priority', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'opened_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'handled_by', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    'invalid_sales': [
        {'name': 'sale_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'sale_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'channel', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'payment_method', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'error_reason', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ],
    'invalid_support_tickets': [
        {'name': 'ticket_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'priority', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'opened_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        {'name': 'handled_by', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'error_reason', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ],
    'invalid_customers': [
        {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'industry', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'error_reason', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ],
    'invalid_products': [
        {'name': 'product_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'description', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'category', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price_usd', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
        {'name': 'active', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
        {'name': 'error_reason', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ],
    'ingestion_metadata': [
        {'name': 'table_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'max_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'last_ingestion_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ],
    'ingestion_errors': [
        {'name': 'table_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'error_message', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'}
    ]
}

# Required columns for each table
REQUIRED_COLUMNS = {
    'sales': ['sale_id', 'customer_id', 'product_id', 'sale_date', 'quantity', 'channel', 'payment_method'],
    'products': ['product_id', 'description', 'category', 'price_usd', 'active'],
    'customers': ['customer_id', 'name', 'country', 'industry', 'registration_date'],
    'support_tickets': ['ticket_id', 'customer_id', 'product_id', 'status', 'priority', 'opened_at', 'handled_by']
} 