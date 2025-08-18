import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

async def check_database():
    DATABASE_URL = "postgresql+psycopg://postgres:fred_password@localhost:5432/postgres"
    engine = create_async_engine(DATABASE_URL, echo=False)
    
    async with engine.begin() as conn:
        print("🔍 CHECKING DATABASE CONTENTS:")
        print("=" * 50)
        
        # Check series
        print("\n📊 FRED SERIES:")
        result = await conn.execute(text("SELECT series_id, title, frequency, units FROM fred_series"))
        for row in result:
            print(f"  {row[0]}: {row[1][:40]}... | {row[2]} | {row[3]}")
        
        # Check observations
        print(f"\n📈 OBSERVATIONS:")
        result = await conn.execute(text("SELECT series_id, observation_date, value FROM fred_observations ORDER BY observation_date DESC"))
        for row in result:
            value = f"${row[2]:,.3f}" if row[2] else "Missing"
            print(f"  {row[0]} | {row[1]} | {value}")
        
        # Check sync logs
        print(f"\n📋 SYNC LOGS:")
        result = await conn.execute(text("SELECT series_id, sync_date, records_added, success FROM fred_sync_log ORDER BY sync_date DESC"))
        for row in result:
            status = "✅" if row[3] else "❌"
            print(f"  {row[1]} | {row[0]} | Added: {row[2]} | {status}")
    
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_database())