# GTM Data Lake System

A comprehensive data lake system for AI-first sales organizations, enabling advanced revenue intelligence through unstructured data analysis and AI-driven insights.

## Architecture Overview

### Target Architecture (LakeSail)
```
[Data Sources] → [API Gateway] → [Message Queue] → [LakeSail Processing] → [Storage Layer] → [AI Layer]
```

### Components
1. **Data Sources**
   - Sales conversations
   - Email threads
   - Product usage data
   - External research

2. **API Gateway**
   - FastAPI-based ingestion endpoints
   - Authentication and rate limiting
   - Request validation

3. **Message Queue**
   - Kafka for real-time data streaming
   - Topic-based routing
   - Message persistence

4. **Processing Layer** (LakeSail)
   - High-performance data processing
   - Batch and stream processing
   - AI workload support

5. **Storage Layer**
   - S3/MinIO for raw data
   - Vector storage for embeddings
   - Metadata catalog

6. **AI Layer**
   - Multiple LLM integration
   - Custom model training
   - Natural language querying

## Project Structure
```
gtm_data_lake/
├── api/                # FastAPI application
├── ingestion/         # Data ingestion modules
├── processing/        # Data processing pipelines
├── ai/               # AI models and agents
├── storage/          # Storage interfaces
└── utils/            # Utility functions
```

## Setup Instructions

### Prerequisites
- Python 3.9+
- Git
- Docker (optional)

### Local Development Setup
1. Clone the repository:
   ```bash
   git clone [repository-url]
   cd gtm-data-lake
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

### Running the Application
1. Start the API server:
   ```bash
   python run_api.py
   ```

2. Access the API documentation:
   - Swagger UI: http://localhost:8000/docs
   - ReDoc: http://localhost:8000/redoc

## LakeSail Integration (Coming Soon)
The system is designed to integrate with LakeSail for high-performance data processing. The integration will be implemented in a future update.

## Development Workflow
1. Create a new branch for features
2. Make changes and commit
3. Push to remote repository
4. Create pull request

## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details. 