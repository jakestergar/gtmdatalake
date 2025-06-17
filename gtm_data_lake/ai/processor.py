from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json
from sentence_transformers import SentenceTransformer
import pinecone
import anthropic
import openai

from ..config import DataLakeConfig

logger = logging.getLogger(__name__)

class AIProcessor:
    """Processor for enriching data with AI-generated insights."""
    
    def __init__(self):
        """Initialize the AI processor."""
        self.config = DataLakeConfig()
        self._initialize_models()
        self._initialize_vector_db()
    
    def _initialize_models(self):
        """Initialize AI models and clients."""
        # Initialize sentence transformer for embeddings
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Initialize LLM clients
        if self.config.OPENAI_API_KEY:
            self.openai_client = openai.OpenAI(api_key=self.config.OPENAI_API_KEY)
        if self.config.ANTHROPIC_API_KEY:
            self.anthropic_client = anthropic.Anthropic(api_key=self.config.ANTHROPIC_API_KEY)
    
    def _initialize_vector_db(self):
        """Initialize vector database connection."""
        if self.config.VECTOR_DB_TYPE == "pinecone":
            pinecone.init(
                api_key=os.getenv("PINECONE_API_KEY"),
                environment=os.getenv("PINECONE_ENVIRONMENT")
            )
            self.vector_db = pinecone.Index(self.config.VECTOR_DB_INDEX)
    
    def process_conversation(self, conversation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and enrich conversation data with AI insights."""
        try:
            # Generate embeddings for the conversation
            transcript = conversation_data['raw_transcript']
            embedding = self.embedding_model.encode(transcript)
            
            # Store embedding in vector database
            self.vector_db.upsert(
                vectors=[{
                    'id': conversation_data['conversation_id'],
                    'values': embedding.tolist(),
                    'metadata': {
                        'timestamp': conversation_data['timestamp'],
                        'company_domain': conversation_data.get('company_domain'),
                        'opportunity_id': conversation_data.get('opportunity_id')
                    }
                }]
            )
            
            # Generate AI insights using Claude
            insights = self._generate_conversation_insights(transcript)
            
            # Add AI insights to the conversation data
            conversation_data['ai_insights'] = insights
            
            return conversation_data
        except Exception as e:
            logger.error(f"Error processing conversation: {str(e)}")
            return conversation_data
    
    def _generate_conversation_insights(self, transcript: str) -> Dict[str, Any]:
        """Generate insights from conversation transcript using Claude."""
        try:
            prompt = f"""Analyze this sales conversation and provide structured insights:

            {transcript}

            Please provide:
            1. Key topics discussed
            2. Sentiment analysis
            3. Action items
            4. Risk factors
            5. Next steps recommended

            Format the response as a JSON object with these keys."""
            
            response = self.anthropic_client.messages.create(
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
            logger.error(f"Error generating conversation insights: {str(e)}")
            return {}
    
    def process_email_thread(self, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and enrich email thread data with AI insights."""
        try:
            # Combine all email bodies
            all_text = " ".join([email['body_text'] for email in email_data['emails']])
            
            # Generate embedding for the thread
            embedding = self.embedding_model.encode(all_text)
            
            # Store embedding in vector database
            self.vector_db.upsert(
                vectors=[{
                    'id': email_data['thread_id'],
                    'values': embedding.tolist(),
                    'metadata': {
                        'timestamp': email_data['emails'][0]['timestamp'],
                        'company_domain': email_data.get('company_domain'),
                        'opportunity_id': email_data.get('opportunity_id')
                    }
                }]
            )
            
            # Generate AI insights for each email
            for email in email_data['emails']:
                email['ai_analysis'] = self._analyze_email(email['body_text'])
            
            return email_data
        except Exception as e:
            logger.error(f"Error processing email thread: {str(e)}")
            return email_data
    
    def _analyze_email(self, email_text: str) -> Dict[str, Any]:
        """Analyze email content using GPT-4."""
        try:
            prompt = f"""Analyze this email and provide structured insights:

            {email_text}

            Please provide:
            1. Sentiment (positive/negative/neutral)
            2. Urgency level (high/medium/low)
            3. Action items
            4. Response required (true/false)

            Format the response as a JSON object with these keys."""
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=0.7,
                max_tokens=500
            )
            
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logger.error(f"Error analyzing email: {str(e)}")
            return {}
    
    def process_product_usage(self, usage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and enrich product usage data with AI insights."""
        try:
            # Generate insights about usage patterns
            events_text = json.dumps(usage_data['events'])
            insights = self._analyze_usage_patterns(events_text)
            
            # Add AI insights to the usage data
            usage_data['ai_insights'] = insights
            
            return usage_data
        except Exception as e:
            logger.error(f"Error processing product usage: {str(e)}")
            return usage_data
    
    def _analyze_usage_patterns(self, events_text: str) -> Dict[str, Any]:
        """Analyze product usage patterns using GPT-4."""
        try:
            prompt = f"""Analyze these product usage events and provide structured insights:

            {events_text}

            Please provide:
            1. Usage patterns identified
            2. Feature adoption analysis
            3. User engagement level
            4. Potential areas for improvement
            5. Success metrics

            Format the response as a JSON object with these keys."""
            
            response = self.openai_client.chat.completions.create(
                model="gpt-4-turbo-preview",
                messages=[{
                    "role": "user",
                    "content": prompt
                }],
                temperature=0.7,
                max_tokens=500
            )
            
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logger.error(f"Error analyzing usage patterns: {str(e)}")
            return {} 