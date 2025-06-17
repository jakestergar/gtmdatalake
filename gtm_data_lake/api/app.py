from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime

from ..config import DataLakeConfig
from ..ingestion.pipeline import DataIngestionPipeline
from ..ai.processor import AIProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="GTM Data Lake API",
    description="API for ingesting and querying GTM data",
    version="1.0.0"
)

# Initialize components
config = DataLakeConfig()
ingestion_pipeline = DataIngestionPipeline()
ai_processor = AIProcessor()

# Pydantic models for request validation
class ConversationData(BaseModel):
    conversation_id: str
    timestamp: str
    raw_transcript: str
    opportunity_id: Optional[str] = None
    company_domain: Optional[str] = None
    participants: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None

class EmailThreadData(BaseModel):
    thread_id: str
    opportunity_id: Optional[str] = None
    company_domain: Optional[str] = None
    emails: List[Dict[str, Any]]

class ProductUsageData(BaseModel):
    user_id: str
    session_id: str
    timestamp: str
    company_domain: Optional[str] = None
    events: List[Dict[str, Any]]
    session_summary: Optional[Dict[str, Any]] = None

class CalendarEventData(BaseModel):
    event_id: str
    title: str
    start_time: str
    end_time: str
    description: Optional[str] = None
    location: Optional[str] = None
    attendees: List[Dict[str, Any]]
    organizer: Dict[str, Any]
    opportunity_id: Optional[str] = None
    company_domain: Optional[str] = None
    meeting_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class AgentData(BaseModel):
    agent_id: str
    agent_type: str
    timestamp: str
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

# API endpoints
@app.post("/ingest/conversation")
async def ingest_conversation(
    data: ConversationData,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Ingest a new conversation."""
    try:
        # Process with AI in background
        background_tasks.add_task(
            ai_processor.process_conversation,
            data.dict()
        )
        
        # Store in data lake
        success = ingestion_pipeline.ingest_conversation(data.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Failed to ingest conversation")
        
        return {
            "status": "success",
            "message": f"Conversation {data.conversation_id} ingested successfully"
        }
    except Exception as e:
        logger.error(f"Error ingesting conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/email-thread")
async def ingest_email_thread(
    data: EmailThreadData,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Ingest a new email thread."""
    try:
        # Process with AI in background
        background_tasks.add_task(
            ai_processor.process_email_thread,
            data.dict()
        )
        
        # Store in data lake
        success = ingestion_pipeline.ingest_email_thread(data.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Failed to ingest email thread")
        
        return {
            "status": "success",
            "message": f"Email thread {data.thread_id} ingested successfully"
        }
    except Exception as e:
        logger.error(f"Error ingesting email thread: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/product-usage")
async def ingest_product_usage(
    data: ProductUsageData,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Ingest new product usage data."""
    try:
        # Process with AI in background
        background_tasks.add_task(
            ai_processor.process_product_usage,
            data.dict()
        )
        
        # Store in data lake
        success = ingestion_pipeline.ingest_product_usage(data.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Failed to ingest product usage data")
        
        return {
            "status": "success",
            "message": f"Product usage data for session {data.session_id} ingested successfully"
        }
    except Exception as e:
        logger.error(f"Error ingesting product usage data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/calendar-event")
async def ingest_calendar_event(
    data: CalendarEventData,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Ingest a new calendar event."""
    try:
        # Process with AI in background
        background_tasks.add_task(
            ai_processor.process_calendar_event,
            data.dict()
        )
        
        # Store in data lake
        success = ingestion_pipeline.ingest_calendar_event(data.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Failed to ingest calendar event")
        
        return {
            "status": "success",
            "message": f"Calendar event {data.event_id} ingested successfully"
        }
    except Exception as e:
        logger.error(f"Error ingesting calendar event: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/agent-data")
async def ingest_agent_data(
    data: AgentData,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Ingest data from an AI agent."""
    try:
        # Process with AI in background
        background_tasks.add_task(
            ai_processor.process_agent_data,
            data.dict()
        )
        
        # Store in data lake
        success = ingestion_pipeline.ingest_agent_data(data.dict())
        if not success:
            raise HTTPException(status_code=500, detail="Failed to ingest agent data")
        
        return {
            "status": "success",
            "message": f"Agent data from {data.agent_type} agent {data.agent_id} ingested successfully"
        }
    except Exception as e:
        logger.error(f"Error ingesting agent data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }

# Start the ingestion pipeline when the application starts
@app.on_event("startup")
async def startup_event():
    """Start the ingestion pipeline on application startup."""
    ingestion_pipeline.start()

# Stop the ingestion pipeline when the application shuts down
@app.on_event("shutdown")
async def shutdown_event():
    """Stop the ingestion pipeline on application shutdown."""
    ingestion_pipeline.stop() 