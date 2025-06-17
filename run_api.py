import uvicorn
from gtm_data_lake.config import DataLakeConfig

if __name__ == "__main__":
    config = DataLakeConfig()
    uvicorn.run(
        "gtm_data_lake.api.app:app",
        host=config.API_HOST,
        port=config.API_PORT,
        reload=True
    ) 