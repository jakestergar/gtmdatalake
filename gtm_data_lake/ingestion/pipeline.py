from typing import Dict, Any, Optional
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import threading

from ..config import DataLakeConfig
from ..storage.storage_client import StorageClient

logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    """Pipeline for ingesting data into the GTM Data Lake."""
    
    def __init__(self):
        """Initialize the ingestion pipeline."""
        self.config = DataLakeConfig()
        self.storage = StorageClient()
        self.consumers = {}
        self.running = False
        self.executor = ThreadPoolExecutor(max_workers=3)
    
    def start(self):
        """Start the ingestion pipeline."""
        self.running = True
        
        # Start Kafka consumers for each data type
        for topic_name, topic in self.config.KAFKA_TOPICS.items():
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            self.consumers[topic_name] = consumer
            
            # Start consumer thread
            thread = threading.Thread(
                target=self._consume_messages,
                args=(topic_name, consumer),
                daemon=True
            )
            thread.start()
    
    def stop(self):
        """Stop the ingestion pipeline."""
        self.running = False
        for consumer in self.consumers.values():
            consumer.close()
        self.executor.shutdown()
    
    def _consume_messages(self, topic_name: str, consumer: KafkaConsumer):
        """Consume messages from a Kafka topic."""
        while self.running:
            try:
                for message in consumer:
                    if not self.running:
                        break
                    
                    # Process message based on topic
                    if topic_name == "conversations":
                        self.executor.submit(self.process_conversation, message.value)
                    elif topic_name == "emails":
                        self.executor.submit(self.process_email_thread, message.value)
                    elif topic_name == "product_usage":
                        self.executor.submit(self.process_product_usage, message.value)
            except Exception as e:
                logger.error(f"Error consuming messages from {topic_name}: {str(e)}")
    
    def process_conversation(self, data: Dict[str, Any]) -> bool:
        """Process and store conversation data."""
        try:
            # Validate required fields
            required_fields = ['conversation_id', 'timestamp', 'raw_transcript']
            if not all(field in data for field in required_fields):
                logger.error(f"Missing required fields in conversation data: {data}")
                return False
            
            # Store in bronze layer
            success = self.storage.store_conversation(data)
            if not success:
                logger.error(f"Failed to store conversation {data['conversation_id']}")
                return False
            
            logger.info(f"Successfully processed conversation {data['conversation_id']}")
            return True
        except Exception as e:
            logger.error(f"Error processing conversation: {str(e)}")
            return False
    
    def process_email_thread(self, data: Dict[str, Any]) -> bool:
        """Process and store email thread data."""
        try:
            # Validate required fields
            required_fields = ['thread_id', 'emails']
            if not all(field in data for field in required_fields):
                logger.error(f"Missing required fields in email thread data: {data}")
                return False
            
            # Store in bronze layer
            success = self.storage.store_email_thread(data)
            if not success:
                logger.error(f"Failed to store email thread {data['thread_id']}")
                return False
            
            logger.info(f"Successfully processed email thread {data['thread_id']}")
            return True
        except Exception as e:
            logger.error(f"Error processing email thread: {str(e)}")
            return False
    
    def process_product_usage(self, data: Dict[str, Any]) -> bool:
        """Process and store product usage data."""
        try:
            # Validate required fields
            required_fields = ['session_id', 'timestamp', 'events']
            if not all(field in data for field in required_fields):
                logger.error(f"Missing required fields in product usage data: {data}")
                return False
            
            # Store in bronze layer
            success = self.storage.store_product_usage(data)
            if not success:
                logger.error(f"Failed to store product usage data for session {data['session_id']}")
                return False
            
            logger.info(f"Successfully processed product usage data for session {data['session_id']}")
            return True
        except Exception as e:
            logger.error(f"Error processing product usage data: {str(e)}")
            return False
    
    def ingest_conversation(self, conversation_data: Dict[str, Any]) -> bool:
        """Directly ingest conversation data (bypassing Kafka)."""
        return self.process_conversation(conversation_data)
    
    def ingest_email_thread(self, email_data: Dict[str, Any]) -> bool:
        """Directly ingest email thread data (bypassing Kafka)."""
        return self.process_email_thread(email_data)
    
    def ingest_product_usage(self, usage_data: Dict[str, Any]) -> bool:
        """Directly ingest product usage data (bypassing Kafka)."""
        return self.process_product_usage(usage_data) 