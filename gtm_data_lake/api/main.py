from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from gtm_data_lake.api.routes import ingestion, query
from gtm_data_lake.ingestion.consumer_service import run_consumer_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="GTM Data Lake API",
    description="API for the GTM Data Lake system",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(ingestion.router, prefix="/api/v1", tags=["ingestion"])
app.include_router(query.router, prefix="/api/v1", tags=["query"])

@app.on_event("startup")
async def startup_event():
    """Startup event handler."""
    try:
        # Start the Kafka consumer service
        consumer_thread = run_consumer_service()
        logger.info("Kafka consumer service started successfully")
    except Exception as e:
        logger.error(f"Error starting Kafka consumer service: {e}")
        raise

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Welcome to the GTM Data Lake API",
        "version": "1.0.0",
        "status": "operational"
    } 