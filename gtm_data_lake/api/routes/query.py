from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging

from gtm_data_lake.ai.query_interface import DataLakeQueryInterface

router = APIRouter()
logger = logging.getLogger(__name__)

# Initialize query interface
query_interface = DataLakeQueryInterface()

@router.post("/query")
async def query_data_lake(question: str) -> Dict[str, Any]:
    """
    Query the data lake using natural language.
    
    Args:
        question: Natural language question about the data
        
    Returns:
        Dict containing the answer and status
    """
    try:
        response = query_interface.query(question)
        if response["status"] == "error":
            raise HTTPException(status_code=500, detail=response["error"])
        return response
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 