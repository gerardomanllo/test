"""Secret Manager integration for configuration."""

from google.cloud import secretmanager
from typing import Optional

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
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error accessing secret {secret_id}: {str(e)}")
        return None

def get_config():
    """
    Get configuration from Secret Manager.
    
    Returns:
        dict: Configuration dictionary with dataset, bucket, and files
    """
    project_id = get_secret("bixlabs", "project_id")
    if not project_id:
        raise ValueError("Could not retrieve project_id from Secret Manager")
        
    dataset = get_secret("bixlabs", "dataset") or "challenge"
    bucket = get_secret("bixlabs", "bucket") or "bixlabs-challenge-bucket"
    files = get_secret("bixlabs", "files")
    
    if files:
        files = files.split(",")
    else:
        files = ["sales.xlsx", "products.xlsx", "customers.xlsx", "support_tickets.xlsx"]
        
    return {
        "project_id": project_id,
        "dataset": dataset,
        "bucket": bucket,
        "files": files
    } 