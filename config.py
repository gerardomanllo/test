"""Configuration management for the ingestion pipeline."""

import os
from google.cloud import secretmanager, logging
from typing import Optional

# Initialize logging client
logging_client = logging.Client()
log = logging_client.logger('bixlabs-ingestion')

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> Optional[str]:
    """
    Get a secret from Secret Manager.
    
    Args:
        project_id: The GCP project ID
        secret_id: The secret ID
        version_id: The version of the secret (defaults to "latest")
        
    Returns:
        The secret value as a string, or None if not found
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        log.log_text(f'Attempting to access secret: {secret_id}', severity='INFO')
        response = client.access_secret_version(request={"name": name})
        value = response.payload.data.decode("UTF-8")
        log.log_text(f'Successfully retrieved secret: {secret_id}', severity='INFO')
        return value
    except Exception as e:
        log.log_text(f"Error accessing secret {secret_id}: {str(e)}", severity='ERROR')
        return None

def get_config():
    """
    Get configuration from Secret Manager.
    
    Returns:
        dict: Configuration dictionary with dataset, bucket, and files
    """
    # Get project_id from environment variable
    project_id = os.environ.get('PROJECT_ID')
    if not project_id:
        log.log_text('PROJECT_ID environment variable not set', severity='ERROR')
        raise ValueError("PROJECT_ID environment variable not set")
        
    log.log_text(f'Using project_id: {project_id}', severity='INFO')
        
    # Get other configuration from Secret Manager
    dataset = get_secret(project_id, "dataset") or "challenge"
    bucket = get_secret(project_id, "bucket") or "bixlabs-challenge-bucket"
    files = get_secret(project_id, "files")
    
    log.log_text(f'Configuration retrieved - dataset: {dataset}, bucket: {bucket}', severity='INFO')
    
    if files:
        files = files.split(",")
        log.log_text(f'Files from secret: {files}', severity='INFO')
    else:
        files = ["sales.xlsx", "products.xlsx", "customers.xlsx", "support_tickets.xlsx"]
        log.log_text(f'Using default files: {files}', severity='INFO')
        
    return {
        "project_id": project_id,
        "dataset": dataset,
        "bucket": bucket,
        "files": files
    } 