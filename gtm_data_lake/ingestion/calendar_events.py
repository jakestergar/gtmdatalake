from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class CalendarEvent(BaseModel):
    """Model for calendar event data."""
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
    meeting_summary: Optional[Dict[str, Any]] = None
    sentiment_analysis: Optional[Dict[str, Any]] = None
    action_items: Optional[List[Dict[str, Any]]] = None
    metadata: Optional[Dict[str, Any]] = None

class CalendarEventProcessor:
    """Processor for calendar events and meeting data."""
    
    def __init__(self, ai_processor):
        """Initialize the calendar event processor."""
        self.ai_processor = ai_processor
    
    def process_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and enrich calendar event data."""
        try:
            # Validate event data
            event = CalendarEvent(**event_data)
            
            # Generate meeting summary if description exists
            if event.description:
                event.meeting_summary = self._generate_meeting_summary(event)
            
            # Generate sentiment analysis
            event.sentiment_analysis = self._analyze_sentiment(event)
            
            # Extract action items
            event.action_items = self._extract_action_items(event)
            
            return event.dict()
        except Exception as e:
            logger.error(f"Error processing calendar event: {str(e)}")
            return event_data
    
    def _generate_meeting_summary(self, event: CalendarEvent) -> Dict[str, Any]:
        """Generate a structured summary of the meeting."""
        try:
            prompt = f"""Analyze this meeting and provide a structured summary:

            Title: {event.title}
            Description: {event.description}
            Attendees: {json.dumps(event.attendees)}
            Duration: {event.start_time} to {event.end_time}

            Please provide:
            1. Key discussion points
            2. Decisions made
            3. Next steps
            4. Follow-up requirements
            5. Risk factors identified

            Format the response as a JSON object with these keys."""
            
            response = self.ai_processor.anthropic_client.messages.create(
                model="claude-3-opus-20240229",
                max_tokens=1000,
                temperature=0.7,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            return json.loads(response.content[0].text)
        except Exception as e:
            logger.error(f"Error generating meeting summary: {str(e)}")
            return {}
    
    def _analyze_sentiment(self, event: CalendarEvent) -> Dict[str, Any]:
        """Analyze sentiment and engagement levels from meeting data."""
        try:
            prompt = f"""Analyze the sentiment and engagement in this meeting:

            Title: {event.title}
            Description: {event.description}
            Attendees: {json.dumps(event.attendees)}

            Please provide:
            1. Overall sentiment (positive/negative/neutral)
            2. Engagement level (high/medium/low)
            3. Key concerns or objections
            4. Enthusiasm indicators
            5. Risk signals

            Format the response as a JSON object with these keys."""
            
            response = self.ai_processor.anthropic_client.messages.create(
                model="claude-3-opus-20240229",
                max_tokens=500,
                temperature=0.7,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            return json.loads(response.content[0].text)
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return {}
    
    def _extract_action_items(self, event: CalendarEvent) -> List[Dict[str, Any]]:
        """Extract action items from meeting data."""
        try:
            prompt = f"""Extract action items from this meeting:

            Title: {event.title}
            Description: {event.description}
            Attendees: {json.dumps(event.attendees)}

            For each action item, provide:
            1. Description
            2. Owner
            3. Due date
            4. Priority
            5. Dependencies

            Format the response as a JSON array of objects with these keys."""
            
            response = self.ai_processor.anthropic_client.messages.create(
                model="claude-3-opus-20240229",
                max_tokens=500,
                temperature=0.7,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            return json.loads(response.content[0].text)
        except Exception as e:
            logger.error(f"Error extracting action items: {str(e)}")
            return [] 