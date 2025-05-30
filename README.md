# Bixlabs Data Ingestion Pipeline

Cloud Function for ingesting Excel files from Google Cloud Storage into BigQuery.

## Overview

This Cloud Function processes Excel files from a GCS bucket and loads them into BigQuery tables. It includes:
- Data validation
- Relationship checking
- Error handling
- Incremental processing for sales data
- Secret Manager integration for configuration

## Prerequisites

- Google Cloud Platform project
- BigQuery dataset
- Cloud Storage bucket
- Secret Manager setup

## Required Secrets

Set up the following secrets in Secret Manager:
- `project_id`: Your GCP project ID
- `dataset`: BigQuery dataset name (defaults to 'challenge')
- `bucket`: GCS bucket name (defaults to 'bixlabs-challenge-bucket')
- `files`: Comma-separated list of Excel files (defaults to all four files)

## Required IAM Permissions

The Cloud Function service account needs:
- `roles/bigquery.dataEditor`
- `roles/bigquery.jobUser`
- `roles/storage.objectViewer`
- `roles/secretmanager.secretAccessor`
- `roles/logging.logWriter`

## Deployment

1. Push code to GitHub
2. Connect repository to Cloud Build
3. Deploy using Cloud Functions:
   ```bash
   gcloud functions deploy bixlabs-ingestion \
     --runtime python39 \
     --trigger-http \
     --entry-point main \
     --source . \
     --memory 512MB \
     --timeout 540s \
     --region us-central1 \
     --service-account YOUR_SERVICE_ACCOUNT@YOUR_PROJECT.iam.gserviceaccount.com
   ```

## Input Files

The function expects Excel files with the following structure:

### sales.xlsx
- sale_id (INTEGER)
- customer_id (INTEGER)
- product_id (INTEGER)
- sale_date (DATE)
- quantity (INTEGER)
- channel (STRING)
- payment_method (STRING)

### products.xlsx
- product_id (INTEGER)
- description (STRING)
- category (STRING)
- price_usd (FLOAT)
- active (BOOLEAN)

### customers.xlsx
- customer_id (INTEGER)
- name (STRING)
- country (STRING)
- industry (STRING)
- registration_date (DATE)

### support_tickets.xlsx
- ticket_id (INTEGER)
- customer_id (INTEGER)
- product_id (INTEGER)
- status (STRING)
- priority (STRING)
- opened_at (TIMESTAMP)
- handled_by (STRING)

## Output Tables

The function creates the following tables in BigQuery:
- `raw_sales`
- `raw_products`
- `raw_customers`
- `raw_support_tickets`
- `invalid_sales`
- `invalid_products`
- `invalid_customers`
- `invalid_support_tickets`
- `ingestion_metadata`
- `ingestion_errors`

## Error Handling

- Invalid records are stored in corresponding `invalid_*` tables
- Errors are logged to Cloud Logging and `ingestion_errors` table
- Failed validations are tracked with error reasons

## Monitoring

Monitor the function using:
- Cloud Logging
- BigQuery `ingestion_errors` table
- Cloud Functions metrics 