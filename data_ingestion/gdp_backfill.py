# Create a file called: gdp_backfill.py

import asyncio
from fred_integration_service import create_ingestion_service

async def backfill_gdp():
    API_KEY = "f418c0623911721d7400fc124662758c"
    DATABASE_URL = "postgresql+psycopg://postgres:fred_password@localhost:5432/postgres"
    
    print("🚀 Starting GDP Historical Backfill...")
    
    service = await create_ingestion_service(API_KEY, DATABASE_URL)
    
    try:
        # The magic parameters for full backfill:
        result = await service.ingest_series(
            series_id="GDP",
            update_existing=False,  # ← This triggers "get everything" logic
            days_lookback=30        # ← This gets ignored when update_existing=False
        )
        
        print(f"✅ Backfill completed!")
        print(f"   Observations added: {result['observations_added']}")
        print(f"   API calls used: {result['api_calls_used']}")
        
    finally:
        await service.close()

if __name__ == "__main__":
    asyncio.run(backfill_gdp())