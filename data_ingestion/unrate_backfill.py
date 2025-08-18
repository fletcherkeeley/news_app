import asyncio
from fred_integration_service import create_ingestion_service

async def backfill_unrate():
    API_KEY = "f418c0623911721d7400fc124662758c"
    DATABASE_URL = "postgresql+psycopg://postgres:fred_password@localhost:5432/postgres"
    
    print("🚀 Starting UNRATE (Unemployment Rate) Historical Backfill...")
    
    service = await create_ingestion_service(API_KEY, DATABASE_URL)
    
    try:
        result = await service.ingest_series(
            series_id="UNRATE",
            update_existing=False,  # Get everything from beginning
            days_lookback=30        # Ignored when update_existing=False
        )
        
        print(f"✅ UNRATE Backfill completed!")
        print(f"   Observations added: {result['observations_added']}")
        print(f"   API calls used: {result['api_calls_used']}")
        
    finally:
        await service.close()

if __name__ == "__main__":
    asyncio.run(backfill_unrate())