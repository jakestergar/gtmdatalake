from typing import Dict, Any
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class KafkaConfig:
    """Configuration settings for Kafka integration."""
    
    # Kafka connection settings
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    
    # Topic configurations
    TOPICS = {
        "conversations": {
            "name": "gtm.conversations",
            "partitions": 3,
            "replication_factor": 1,
            "retention_hours": 168  # 7 days
        },
        "emails": {
            "name": "gtm.emails",
            "partitions": 3,
            "replication_factor": 1,
            "retention_hours": 168
        },
        "product_usage": {
            "name": "gtm.product_usage",
            "partitions": 3,
            "replication_factor": 1,
            "retention_hours": 168
        }
    }
    
    # Consumer group settings
    CONSUMER_GROUP = "gtm-data-lake-group"
    
    # Producer settings
    PRODUCER_CONFIG: Dict[str, Any] = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "security_protocol": SECURITY_PROTOCOL,
        "value_serializer": lambda x: x.encode('utf-8'),
        "acks": "all",
        "retries": 3
    }
    
    # Consumer settings
    CONSUMER_CONFIG: Dict[str, Any] = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "security_protocol": SECURITY_PROTOCOL,
        "group_id": CONSUMER_GROUP,
        "auto_offset_reset": "latest",
        "enable_auto_commit": True,
        "value_deserializer": lambda x: x.decode('utf-8')
    }
    
    @classmethod
    def get_topic_name(cls, topic_type: str) -> str:
        """Get the full topic name for a given type."""
        if topic_type not in cls.TOPICS:
            raise ValueError(f"Unknown topic type: {topic_type}")
        return cls.TOPICS[topic_type]["name"]
    
    @classmethod
    def get_topic_config(cls, topic_type: str) -> Dict[str, Any]:
        """Get the configuration for a specific topic."""
        if topic_type not in cls.TOPICS:
            raise ValueError(f"Unknown topic type: {topic_type}")
        return cls.TOPICS[topic_type] 