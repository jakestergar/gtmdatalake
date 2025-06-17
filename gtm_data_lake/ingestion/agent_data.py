from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class AgentData(BaseModel):
    """Base model for agent data."""
    agent_id: str
    agent_type: str
    timestamp: str
    data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

class LeadQualificationData(AgentData):
    """Data from Initial Lead Qualification Agent."""
    agent_type: str = "lead_qualification"
    data: Dict[str, Any] = Field(..., description="Lead qualification data including domain verification, company size, etc.")

class AccountIntelligenceData(AgentData):
    """Data from Comprehensive Account Intelligence Gathering Agent."""
    agent_type: str = "account_intelligence"
    data: Dict[str, Any] = Field(..., description="Account intelligence data including user mapping, usage patterns, etc.")

class SalesProcessData(AgentData):
    """Data from Sales Process Analysis Agent."""
    agent_type: str = "sales_process"
    data: Dict[str, Any] = Field(..., description="Sales process data including BANT, MEDDPIC, stage progression, etc.")

class SentimentAnalysisData(AgentData):
    """Data from Sentiment and Engagement Analysis Agent."""
    agent_type: str = "sentiment_analysis"
    data: Dict[str, Any] = Field(..., description="Sentiment analysis data including engagement levels, concerns, etc.")

class ProductIntelligenceData(AgentData):
    """Data from Product Intelligence Extraction Agent."""
    agent_type: str = "product_intelligence"
    data: Dict[str, Any] = Field(..., description="Product intelligence data including feature requests, use cases, etc.")

class FollowUpData(AgentData):
    """Data from Follow-up Automation Agent."""
    agent_type: str = "follow_up"
    data: Dict[str, Any] = Field(..., description="Follow-up data including email drafts, calendar events, etc.")

class MarketingIntelligenceData(AgentData):
    """Data from Marketing Intelligence Extraction Agent."""
    agent_type: str = "marketing_intelligence"
    data: Dict[str, Any] = Field(..., description="Marketing intelligence data including industry classification, use cases, etc.")

class ForecastData(AgentData):
    """Data from Forecasting Intelligence Agents."""
    agent_type: str = "forecast"
    data: Dict[str, Any] = Field(..., description="Forecast data including close date predictions, probability assessments, etc.")

class OutcomeAnalysisData(AgentData):
    """Data from Outcome Analysis Agents."""
    agent_type: str = "outcome_analysis"
    data: Dict[str, Any] = Field(..., description="Outcome analysis data including win/loss analysis, success factors, etc.")

class AgentDataProcessor:
    """Processor for handling data from various AI agents."""
    
    def __init__(self, storage_client):
        """Initialize the agent data processor."""
        self.storage_client = storage_client
        self.agent_handlers = {
            "lead_qualification": self._process_lead_qualification,
            "account_intelligence": self._process_account_intelligence,
            "sales_process": self._process_sales_process,
            "sentiment_analysis": self._process_sentiment_analysis,
            "product_intelligence": self._process_product_intelligence,
            "follow_up": self._process_follow_up,
            "marketing_intelligence": self._process_marketing_intelligence,
            "forecast": self._process_forecast,
            "outcome_analysis": self._process_outcome_analysis
        }
    
    def process_agent_data(self, data: Dict[str, Any]) -> bool:
        """Process data from any agent type."""
        try:
            # Validate and parse the agent data
            agent_type = data.get("agent_type")
            if agent_type not in self.agent_handlers:
                logger.error(f"Unknown agent type: {agent_type}")
                return False
            
            # Process the data using the appropriate handler
            handler = self.agent_handlers[agent_type]
            processed_data = handler(data)
            
            # Store the processed data
            return self.storage_client.store_agent_data(processed_data)
        except Exception as e:
            logger.error(f"Error processing agent data: {str(e)}")
            return False
    
    def _process_lead_qualification(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process lead qualification data."""
        try:
            lead_data = LeadQualificationData(**data)
            # Add any additional processing specific to lead qualification
            return lead_data.dict()
        except Exception as e:
            logger.error(f"Error processing lead qualification data: {str(e)}")
            return data
    
    def _process_account_intelligence(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process account intelligence data."""
        try:
            account_data = AccountIntelligenceData(**data)
            # Add any additional processing specific to account intelligence
            return account_data.dict()
        except Exception as e:
            logger.error(f"Error processing account intelligence data: {str(e)}")
            return data
    
    def _process_sales_process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process sales process data."""
        try:
            sales_data = SalesProcessData(**data)
            # Add any additional processing specific to sales process
            return sales_data.dict()
        except Exception as e:
            logger.error(f"Error processing sales process data: {str(e)}")
            return data
    
    def _process_sentiment_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process sentiment analysis data."""
        try:
            sentiment_data = SentimentAnalysisData(**data)
            # Add any additional processing specific to sentiment analysis
            return sentiment_data.dict()
        except Exception as e:
            logger.error(f"Error processing sentiment analysis data: {str(e)}")
            return data
    
    def _process_product_intelligence(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process product intelligence data."""
        try:
            product_data = ProductIntelligenceData(**data)
            # Add any additional processing specific to product intelligence
            return product_data.dict()
        except Exception as e:
            logger.error(f"Error processing product intelligence data: {str(e)}")
            return data
    
    def _process_follow_up(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process follow-up data."""
        try:
            follow_up_data = FollowUpData(**data)
            # Add any additional processing specific to follow-up
            return follow_up_data.dict()
        except Exception as e:
            logger.error(f"Error processing follow-up data: {str(e)}")
            return data
    
    def _process_marketing_intelligence(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process marketing intelligence data."""
        try:
            marketing_data = MarketingIntelligenceData(**data)
            # Add any additional processing specific to marketing intelligence
            return marketing_data.dict()
        except Exception as e:
            logger.error(f"Error processing marketing intelligence data: {str(e)}")
            return data
    
    def _process_forecast(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process forecast data."""
        try:
            forecast_data = ForecastData(**data)
            # Add any additional processing specific to forecasting
            return forecast_data.dict()
        except Exception as e:
            logger.error(f"Error processing forecast data: {str(e)}")
            return data
    
    def _process_outcome_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process outcome analysis data."""
        try:
            outcome_data = OutcomeAnalysisData(**data)
            # Add any additional processing specific to outcome analysis
            return outcome_data.dict()
        except Exception as e:
            logger.error(f"Error processing outcome analysis data: {str(e)}")
            return data 