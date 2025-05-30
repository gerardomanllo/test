"""Configuration management for the ingestion pipeline."""

import os
from google.cloud import secretmanager
from typing import Optional
from utils import add_to_log, log_error

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
        add_to_log(f'Attempting to access secret: {secret_id}')
        response = client.access_secret_version(request={"name": name})
        value = response.payload.data.decode("UTF-8")
        add_to_log(f'Successfully retrieved secret: {secret_id}')
        return value
    except Exception as e:
        log_error('config', f"Error accessing secret {secret_id}: {str(e)}")
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
        log_error('config', 'PROJECT_ID environment variable not set')
        raise ValueError("PROJECT_ID environment variable not set")
        
    add_to_log(f'Using project_id: {project_id}')
        
    # Get other configuration from Secret Manager
    dataset = get_secret(project_id, "dataset") or "challenge"
    bucket = get_secret(project_id, "bucket") or "bixlabs-challenge-bucket"
    files = get_secret(project_id, "files")
    
    add_to_log(f'Configuration retrieved - dataset: {dataset}, bucket: {bucket}')
    
    if files:
        files = files.split(",")
        add_to_log(f'Files from secret: {files}')
    else:
        files = ["sales.xlsx", "products.xlsx", "customers.xlsx", "support_tickets.xlsx"]
        add_to_log(f'Using default files: {files}')
        
    return {
        "project_id": project_id,
        "dataset": dataset,
        "bucket": bucket,
        "files": files
    } 