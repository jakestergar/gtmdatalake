from pathlib import Path
from typing import Dict, Any
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DataLakeConfig:
    """Configuration settings for the GTM Data Lake system."""
    
    # Storage Configuration
    STORAGE_TYPE = os.getenv("STORAGE_TYPE", "s3")  # s3 or minio
    STORAGE_BUCKET = os.getenv("STORAGE_BUCKET", "gtm-data-lake")
    STORAGE_REGION = os.getenv("STORAGE_REGION", "us-west-2")
    
    # Vector Database Configuration
    VECTOR_DB_TYPE = os.getenv("VECTOR_DB_TYPE", "pinecone")  # pinecone or weaviate
    VECTOR_DB_INDEX = os.getenv("VECTOR_DB_INDEX", "gtm-embeddings")
    
    # AI Model Configuration
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    
    # Data Lake Paths
    BASE_PATH = Path("s3://gtm-data-lake")
    BRONZE_PATH = BASE_PATH / "bronze"
    SILVER_PATH = BASE_PATH / "silver"
    GOLD_PATH = BASE_PATH / "gold"
    
    # Data Processing Configuration
    SPARK_CONFIG: Dict[str, Any] = {
        "spark.sql.warehouse.dir": "s3://gtm-data-lake/warehouse",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.gtm": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.gtm.warehouse": "s3://gtm-data-lake/warehouse",
        "spark.sql.catalog.gtm.type": "hive",
    }
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPICS = {
        "conversations": "gtm.conversations",
        "emails": "gtm.emails",
        "product_usage": "gtm.product_usage",
        "calendar_events": "gtm.calendar_events",
        "agent_data": "gtm.agent_data"
    }
    
    # API Configuration
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    
    @classmethod
    def get_storage_path(cls, layer: str, data_type: str, year: int, month: int, day: int) -> Path:
        """Generate the storage path for a specific data type and date."""
        base = getattr(cls, f"{layer.upper()}_PATH")
        return base / data_type / f"year={year}/month={month}/day={day}"
    
    @classmethod
    def get_conversation_path(cls, year: int, month: int, day: int) -> Path:
        """Get the path for conversation data."""
        return cls.get_storage_path("bronze", "conversations", year, month, day)
    
    @classmethod
    def get_email_path(cls, year: int, month: int, day: int) -> Path:
        """Get the path for email data."""
        return cls.get_storage_path("bronze", "emails", year, month, day)
    
    @classmethod
    def get_product_usage_path(cls, year: int, month: int, day: int) -> Path:
        """Get the path for product usage data."""
        return cls.get_storage_path("bronze", "product_usage", year, month, day)
    
    def get_calendar_path(self, year: int, month: int, day: int) -> Path:
        """Get the path for calendar events."""
        return self.BRONZE_PATH / "calendar_events" / str(year) / f"{month:02d}" / f"{day:02d}"
    
    def get_agent_path(self, agent_type: str, year: int, month: int, day: int) -> Path:
        """Get the path for agent data."""
        return self.BRONZE_PATH / "agent_data" / agent_type / str(year) / f"{month:02d}" / f"{day:02d}" 