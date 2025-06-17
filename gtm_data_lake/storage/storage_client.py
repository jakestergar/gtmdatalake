import boto3
from botocore.exceptions import ClientError
from pathlib import Path
import json
from typing import Any, Dict, Optional, Union
import logging
from datetime import datetime

from ..config import DataLakeConfig

logger = logging.getLogger(__name__)

class StorageClient:
    """Client for interacting with S3/MinIO storage."""
    
    def __init__(self):
        """Initialize the storage client."""
        self.config = DataLakeConfig()
        self.client = self._initialize_client()
    
    def _initialize_client(self) -> boto3.client:
        """Initialize the S3/MinIO client."""
        if self.config.STORAGE_TYPE == "minio":
            return boto3.client(
                's3',
                endpoint_url=f"http://{self.config.STORAGE_BUCKET}",
                aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
                region_name=self.config.STORAGE_REGION
            )
        else:
            return boto3.client('s3', region_name=self.config.STORAGE_REGION)
    
    def store_json(self, path: Union[str, Path], data: Dict[str, Any]) -> bool:
        """Store JSON data at the specified path."""
        try:
            path = str(path)
            self.client.put_object(
                Bucket=self.config.STORAGE_BUCKET,
                Key=path,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Successfully stored JSON data at {path}")
            return True
        except ClientError as e:
            logger.error(f"Error storing JSON data at {path}: {str(e)}")
            return False
    
    def read_json(self, path: Union[str, Path]) -> Optional[Dict[str, Any]]:
        """Read JSON data from the specified path."""
        try:
            path = str(path)
            response = self.client.get_object(
                Bucket=self.config.STORAGE_BUCKET,
                Key=path
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            logger.error(f"Error reading JSON data from {path}: {str(e)}")
            return None
    
    def store_conversation(self, conversation_data: Dict[str, Any]) -> bool:
        """Store conversation data in the bronze layer."""
        timestamp = datetime.fromisoformat(conversation_data['timestamp'])
        path = self.config.get_conversation_path(
            timestamp.year,
            timestamp.month,
            timestamp.day
        ) / f"call_{timestamp.strftime('%Y%m%d_%H%M%S')}_{conversation_data['conversation_id']}.json"
        
        return self.store_json(path, conversation_data)
    
    def store_email_thread(self, email_data: Dict[str, Any]) -> bool:
        """Store email thread data in the bronze layer."""
        timestamp = datetime.fromisoformat(email_data['emails'][0]['timestamp'])
        path = self.config.get_email_path(
            timestamp.year,
            timestamp.month,
            timestamp.day
        ) / f"email_thread_{email_data['thread_id']}.json"
        
        return self.store_json(path, email_data)
    
    def store_product_usage(self, usage_data: Dict[str, Any]) -> bool:
        """Store product usage data in the bronze layer."""
        timestamp = datetime.fromisoformat(usage_data['timestamp'])
        path = self.config.get_product_usage_path(
            timestamp.year,
            timestamp.month,
            timestamp.day
        ) / f"user_events_{usage_data['session_id']}.json"
        
        return self.store_json(path, usage_data)
    
    def list_objects(self, prefix: str) -> list:
        """List objects in the bucket with the given prefix."""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.config.STORAGE_BUCKET,
                Prefix=prefix
            )
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            logger.error(f"Error listing objects with prefix {prefix}: {str(e)}")
            return []
    
    def delete_object(self, path: Union[str, Path]) -> bool:
        """Delete an object from the bucket."""
        try:
            path = str(path)
            self.client.delete_object(
                Bucket=self.config.STORAGE_BUCKET,
                Key=path
            )
            logger.info(f"Successfully deleted object at {path}")
            return True
        except ClientError as e:
            logger.error(f"Error deleting object at {path}: {str(e)}")
            return False 