import logging
from typing import Dict, Any
import json
from threading import Thread

from gtm_data_lake.ingestion.kafka_client import KafkaConsumerClient
from gtm_data_lake.storage.storage_client import StorageClient

logger = logging.getLogger(__name__)

class ConsumerService:
    """Service for consuming and processing messages from Kafka topics."""
    
    def __init__(self):
        """Initialize the consumer service."""
        self.storage_client = StorageClient()
        self.consumer = KafkaConsumerClient(
            topic_types=["conversations", "emails", "product_usage"],
            message_handler=self._handle_message
        )
    
    def _handle_message(self, topic: str, message: Dict[str, Any]):
        """
        Handle incoming messages from Kafka topics.
        
        Args:
            topic: Kafka topic name
            message: Message content
        """
        try:
            # Determine the data type based on the topic
            if "conversations" in topic:
                self._process_conversation(message)
            elif "emails" in topic:
                self._process_email(message)
            elif "product_usage" in topic:
                self._process_product_usage(message)
            else:
                logger.warning(f"Unknown topic: {topic}")
        except Exception as e:
            logger.error(f"Error processing message from {topic}: {e}")
    
    def _process_conversation(self, message: Dict[str, Any]):
        """Process a conversation message."""
        try:
            # Store in bronze layer
            self.storage_client.store_bronze(
                "conversations",
                message,
                "json"
            )
            logger.info("Conversation processed and stored successfully")
        except Exception as e:
            logger.error(f"Error processing conversation: {e}")
            raise
    
    def _process_email(self, message: Dict[str, Any]):
        """Process an email message."""
        try:
            # Store in bronze layer
            self.storage_client.store_bronze(
                "emails",
                message,
                "json"
            )
            logger.info("Email processed and stored successfully")
        except Exception as e:
            logger.error(f"Error processing email: {e}")
            raise
    
    def _process_product_usage(self, message: Dict[str, Any]):
        """Process a product usage message."""
        try:
            # Store in bronze layer
            self.storage_client.store_bronze(
                "product_usage",
                message,
                "json"
            )
            logger.info("Product usage data processed and stored successfully")
        except Exception as e:
            logger.error(f"Error processing product usage data: {e}")
            raise
    
    def start(self):
        """Start consuming messages."""
        try:
            logger.info("Starting consumer service...")
            self.consumer.start_consuming()
        except Exception as e:
            logger.error(f"Error in consumer service: {e}")
            raise
        finally:
            self.storage_client.close()

def run_consumer_service():
    """Run the consumer service in a separate thread."""
    service = ConsumerService()
    thread = Thread(target=service.start)
    thread.daemon = True
    thread.start()
    return thread 