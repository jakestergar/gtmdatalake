from typing import Dict, Any, Callable, Optional
import json
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from gtm_data_lake.config.kafka_config import KafkaConfig

logger = logging.getLogger(__name__)

class KafkaProducerClient:
    """Kafka producer client for sending messages to topics."""
    
    def __init__(self):
        """Initialize the Kafka producer."""
        try:
            self.producer = KafkaProducer(**KafkaConfig.PRODUCER_CONFIG)
            logger.info("Kafka producer initialized successfully")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def send_message(self, topic_type: str, message: Dict[str, Any]) -> bool:
        """
        Send a message to a Kafka topic.
        
        Args:
            topic_type: Type of topic (conversations, emails, product_usage)
            message: Message to send
            
        Returns:
            bool: True if message was sent successfully
        """
        try:
            topic = KafkaConfig.get_topic_name(topic_type)
            message_str = json.dumps(message)
            future = self.producer.send(topic, value=message_str)
            self.producer.flush()
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic_type}: {e}")
            return False
    
    def close(self):
        """Close the Kafka producer connection."""
        try:
            self.producer.close()
            logger.info("Kafka producer closed successfully")
        except KafkaError as e:
            logger.error(f"Error closing Kafka producer: {e}")

class KafkaConsumerClient:
    """Kafka consumer client for receiving messages from topics."""
    
    def __init__(self, topic_types: list[str], message_handler: Callable[[str, Dict[str, Any]], None]):
        """
        Initialize the Kafka consumer.
        
        Args:
            topic_types: List of topic types to consume from
            message_handler: Callback function to handle received messages
        """
        try:
            topics = [KafkaConfig.get_topic_name(topic_type) for topic_type in topic_types]
            self.consumer = KafkaConsumer(
                *topics,
                **KafkaConfig.CONSUMER_CONFIG
            )
            self.message_handler = message_handler
            logger.info(f"Kafka consumer initialized for topics: {topics}")
        except KafkaError as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def start_consuming(self):
        """Start consuming messages from Kafka topics."""
        try:
            for message in self.consumer:
                try:
                    topic = message.topic
                    value = json.loads(message.value)
                    self.message_handler(topic, value)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except KafkaError as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close the Kafka consumer connection."""
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed successfully")
        except KafkaError as e:
            logger.error(f"Error closing Kafka consumer: {e}") 