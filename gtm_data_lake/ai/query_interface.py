from typing import Dict, Any, List, Optional
import logging
from datetime import datetime, timedelta
import json
from pathlib import Path

from langchain.agents import Tool, AgentExecutor, LLMSingleActionAgent
from langchain.prompts import StringPromptTemplate
from langchain.chains import LLMChain
from langchain.memory import ConversationBufferMemory
from langchain.schema import AgentAction, AgentFinish
import anthropic
import openai

from ..config import DataLakeConfig
from ..storage.storage_client import StorageClient

logger = logging.getLogger(__name__)

class DataLakeQueryInterface:
    """Interface for natural language querying of the GTM Data Lake."""
    
    def __init__(self):
        """Initialize the query interface."""
        self.config = DataLakeConfig()
        self.storage = StorageClient()
        self._initialize_llm()
        self._initialize_tools()
        self._initialize_agent()
    
    def _initialize_llm(self):
        """Initialize the language model."""
        if self.config.ANTHROPIC_API_KEY:
            self.llm = anthropic.Anthropic(api_key=self.config.ANTHROPIC_API_KEY)
        else:
            self.llm = openai.OpenAI(api_key=self.config.OPENAI_API_KEY)
    
    def _initialize_tools(self):
        """Initialize the tools available to the agent."""
        self.tools = [
            Tool(
                name="search_conversations",
                func=self._search_conversations,
                description="Search for sales conversations based on criteria like date range, company, or opportunity"
            ),
            Tool(
                name="analyze_conversation",
                func=self._analyze_conversation,
                description="Analyze a specific conversation for insights about topics, sentiment, and outcomes"
            ),
            Tool(
                name="get_opportunity_forecast",
                func=self._get_opportunity_forecast,
                description="Get forecasted opportunities with their expected close dates and values"
            ),
            Tool(
                name="search_emails",
                func=self._search_emails,
                description="Search for email threads related to specific opportunities or companies"
            ),
            Tool(
                name="get_product_usage",
                func=self._get_product_usage,
                description="Get product usage data for specific companies or time periods"
            )
        ]
    
    def _initialize_agent(self):
        """Initialize the LangChain agent."""
        prompt = DataLakePromptTemplate(
            tools=self.tools,
            template="""You are an AI assistant that helps founders and sales teams understand their sales data.
            You have access to a data lake containing sales conversations, emails, and product usage data.
            
            Use the following tools to answer questions:
            {tools}
            
            Current conversation:
            {chat_history}
            
            Human: {input}
            {agent_scratchpad}
            """
        )
        
        llm_chain = LLMChain(llm=self.llm, prompt=prompt)
        
        self.agent = LLMSingleActionAgent(
            llm_chain=llm_chain,
            allowed_tools=[tool.name for tool in self.tools],
            stop=["\nObservation:"],
            memory=ConversationBufferMemory(memory_key="chat_history")
        )
        
        self.agent_executor = AgentExecutor.from_agent_and_tools(
            agent=self.agent,
            tools=self.tools,
            verbose=True,
            memory=self.agent.memory
        )
    
    def query(self, question: str) -> Dict[str, Any]:
        """Process a natural language question about the data lake."""
        try:
            response = self.agent_executor.run(question)
            return {
                "answer": response,
                "status": "success"
            }
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return {
                "answer": "I encountered an error while processing your question. Please try rephrasing it.",
                "status": "error",
                "error": str(e)
            }
    
    def _search_conversations(self, query: str) -> List[Dict[str, Any]]:
        """Search for conversations based on natural language query."""
        try:
            # Parse the query to extract date ranges, companies, etc.
            parsed_query = self._parse_search_query(query)
            
            # Search the data lake
            conversations = []
            start_date = parsed_query.get('start_date', datetime.now() - timedelta(days=30))
            end_date = parsed_query.get('end_date', datetime.now())
            
            # Walk through the bronze layer for conversations
            base_path = self.config.BRONZE_PATH / "conversations"
            for path in base_path.rglob("*.json"):
                if self._is_within_date_range(path, start_date, end_date):
                    data = self.storage.read_json(path)
                    if data and self._matches_criteria(data, parsed_query):
                        conversations.append(data)
            
            return conversations
        except Exception as e:
            logger.error(f"Error searching conversations: {str(e)}")
            return []
    
    def _analyze_conversation(self, conversation_id: str) -> Dict[str, Any]:
        """Analyze a specific conversation for insights."""
        try:
            # Find the conversation file
            conversation = None
            base_path = self.config.BRONZE_PATH / "conversations"
            for path in base_path.rglob(f"*{conversation_id}.json"):
                conversation = self.storage.read_json(path)
                break
            
            if not conversation:
                return {"error": "Conversation not found"}
            
            # Generate insights using Claude
            prompt = f"""Analyze this sales conversation and provide detailed insights:

            {conversation['raw_transcript']}

            Please provide:
            1. Key topics discussed
            2. Sentiment analysis
            3. Action items
            4. Risk factors
            5. Next steps recommended
            6. Success indicators
            7. Areas for improvement

            Format the response as a JSON object with these keys."""
            
            response = self.llm.messages.create(
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
            logger.error(f"Error analyzing conversation: {str(e)}")
            return {"error": str(e)}
    
    def _get_opportunity_forecast(self, query: str) -> List[Dict[str, Any]]:
        """Get forecasted opportunities based on natural language query."""
        try:
            # Parse the query to extract date range and other criteria
            parsed_query = self._parse_search_query(query)
            
            # Search for opportunities in the data lake
            opportunities = []
            base_path = self.config.SILVER_PATH / "opportunities"
            
            for path in base_path.rglob("*.json"):
                data = self.storage.read_json(path)
                if data and self._matches_opportunity_criteria(data, parsed_query):
                    opportunities.append(data)
            
            # Sort by forecasted value
            opportunities.sort(key=lambda x: x.get('forecasted_value', 0), reverse=True)
            
            return opportunities[:10]  # Return top 10
        except Exception as e:
            logger.error(f"Error getting opportunity forecast: {str(e)}")
            return []
    
    def _search_emails(self, query: str) -> List[Dict[str, Any]]:
        """Search for email threads based on natural language query."""
        try:
            # Parse the query to extract criteria
            parsed_query = self._parse_search_query(query)
            
            # Search the data lake
            emails = []
            base_path = self.config.BRONZE_PATH / "emails"
            
            for path in base_path.rglob("*.json"):
                data = self.storage.read_json(path)
                if data and self._matches_criteria(data, parsed_query):
                    emails.append(data)
            
            return emails
        except Exception as e:
            logger.error(f"Error searching emails: {str(e)}")
            return []
    
    def _get_product_usage(self, query: str) -> Dict[str, Any]:
        """Get product usage data based on natural language query."""
        try:
            # Parse the query to extract criteria
            parsed_query = self._parse_search_query(query)
            
            # Search the data lake
            usage_data = []
            base_path = self.config.BRONZE_PATH / "product_usage"
            
            for path in base_path.rglob("*.json"):
                data = self.storage.read_json(path)
                if data and self._matches_criteria(data, parsed_query):
                    usage_data.append(data)
            
            # Aggregate usage data
            return self._aggregate_usage_data(usage_data)
        except Exception as e:
            logger.error(f"Error getting product usage: {str(e)}")
            return {}
    
    def _parse_search_query(self, query: str) -> Dict[str, Any]:
        """Parse a natural language query into structured criteria."""
        try:
            prompt = f"""Parse this search query into structured criteria:

            {query}

            Extract:
            1. Date ranges
            2. Company names
            3. Opportunity IDs
            4. Other relevant filters

            Format the response as a JSON object."""
            
            response = self.llm.messages.create(
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
            logger.error(f"Error parsing search query: {str(e)}")
            return {}
    
    def _is_within_date_range(self, path: Path, start_date: datetime, end_date: datetime) -> bool:
        """Check if a file's date is within the specified range."""
        try:
            # Extract date from path (format: year=YYYY/month=MM/day=DD)
            parts = path.parts
            year = int(parts[-4].split('=')[1])
            month = int(parts[-3].split('=')[1])
            day = int(parts[-2].split('=')[1])
            file_date = datetime(year, month, day)
            return start_date <= file_date <= end_date
        except Exception:
            return False
    
    def _matches_criteria(self, data: Dict[str, Any], criteria: Dict[str, Any]) -> bool:
        """Check if data matches the search criteria."""
        try:
            # Check company domain
            if 'company_domain' in criteria and data.get('company_domain') != criteria['company_domain']:
                return False
            
            # Check opportunity ID
            if 'opportunity_id' in criteria and data.get('opportunity_id') != criteria['opportunity_id']:
                return False
            
            # Add more criteria checks as needed
            
            return True
        except Exception:
            return False
    
    def _matches_opportunity_criteria(self, data: Dict[str, Any], criteria: Dict[str, Any]) -> bool:
        """Check if an opportunity matches the search criteria."""
        try:
            # Check forecasted close date
            if 'close_date' in criteria:
                close_date = datetime.fromisoformat(data.get('forecasted_close_date', ''))
                if close_date > criteria['close_date']:
                    return False
            
            # Check forecasted value
            if 'min_value' in criteria and data.get('forecasted_value', 0) < criteria['min_value']:
                return False
            
            # Add more criteria checks as needed
            
            return True
        except Exception:
            return False
    
    def _aggregate_usage_data(self, usage_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Aggregate product usage data into meaningful metrics."""
        try:
            # Implement aggregation logic here
            return {
                "total_sessions": len(usage_data),
                "unique_users": len(set(d['user_id'] for d in usage_data)),
                "total_events": sum(len(d['events']) for d in usage_data),
                # Add more metrics as needed
            }
        except Exception as e:
            logger.error(f"Error aggregating usage data: {str(e)}")
            return {}

class DataLakePromptTemplate(StringPromptTemplate):
    """Custom prompt template for the data lake agent."""
    
    def __init__(self, tools: List[Tool], template: str):
        """Initialize the prompt template."""
        super().__init__(template=template, input_variables=["input", "chat_history", "agent_scratchpad"])
        self.tools = tools
    
    def format(self, **kwargs) -> str:
        """Format the prompt with the given variables."""
        # Format the tools
        tools_str = "\n".join([f"{tool.name}: {tool.description}" for tool in self.tools])
        
        # Format the prompt
        return self.template.format(
            tools=tools_str,
            **kwargs
        ) 