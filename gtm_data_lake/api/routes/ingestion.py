from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import logging

from gtm_data_lake.ingestion.kafka_client import KafkaProducerClient
from gtm_data_lake.config.kafka_config import KafkaConfig

router = APIRouter()
logger = logging.getLogger(__name__)

# Initialize Kafka producer
kafka_producer = KafkaProducerClient()

@router.post("/ingest/conversation")
async def ingest_conversation(data: Dict[str, Any]):
    """
    Ingest a conversation into the data lake.
    
    Args:
        data: Conversation data to ingest
        
    Returns:
        Dict containing ingestion status
    """
    try:
        success = kafka_producer.send_message("conversations", data)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to send message to Kafka")
        return {"status": "success", "message": "Conversation ingested successfully"}
    except Exception as e:
        logger.error(f"Error ingesting conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingest/email")
async def ingest_email(data: Dict[str, Any]):
    """
    Ingest an email thread into the data lake.
    
    Args:
        data: Email data to ingest
        
    Returns:
        Dict containing ingestion status
    """
    try:
        success = kafka_producer.send_message("emails", data)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to send message to Kafka")
        return {"status": "success", "message": "Email ingested successfully"}
    except Exception as e:
        logger.error(f"Error ingesting email: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/ingest/product-usage")
async def ingest_product_usage(data: Dict[str, Any]):
    """
    Ingest product usage data into the data lake.
    
    Args:
        data: Product usage data to ingest
        
    Returns:
        Dict containing ingestion status
    """
    try:
        success = kafka_producer.send_message("product_usage", data)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to send message to Kafka")
        return {"status": "success", "message": "Product usage data ingested successfully"}
    except Exception as e:
        logger.error(f"Error ingesting product usage data: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 