import asyncio
from sqlalchemy import Column, String, Integer, Numeric, Date, DateTime, Text, Boolean, ForeignKey
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

Base = declarative_base()

class FredSeries(Base):
    __tablename__ = "fred_series"
    
    series_id = Column(String(50), primary_key=True)
    title = Column(String(500), nullable=False)
    category = Column(String(100))
    subcategory = Column(String(100))
    units = Column(String(200))
    units_short = Column(String(50))
    frequency = Column(String(20))  # Daily, Weekly, Monthly, Quarterly, Annual
    frequency_short = Column(String(5))  # D, W, M, Q, A
    seasonal_adjustment = Column(String(50))
    source = Column(String(200))
    notes = Column(Text)
    observation_start = Column(Date)
    observation_end = Column(Date)
    last_updated = Column(DateTime)
    popularity = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to observations
    observations = relationship("FredObservation", back_populates="series")

class FredObservation(Base):
    __tablename__ = "fred_observations"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(String(50), ForeignKey("fred_series.series_id"), nullable=False)
    observation_date = Column(Date, nullable=False)
    value = Column(Numeric(20, 6))  # Allows NULL for missing data points
    realtime_start = Column(Date)
    realtime_end = Column(Date)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to series
    series = relationship("FredSeries", back_populates="observations")

class FredSyncLog(Base):
    __tablename__ = "fred_sync_log"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(String(50), ForeignKey("fred_series.series_id"))
    sync_date = Column(DateTime, default=datetime.utcnow)
    records_added = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    success = Column(Boolean, default=True)
    error_message = Column(Text)
    api_calls_used = Column(Integer, default=1)

async def create_tables():
    """Create all tables in the database"""
    DATABASE_URL = "postgresql+psycopg://postgres:fred_password@localhost:5432/postgres"
    
    # Create async engine
    engine = create_async_engine(DATABASE_URL, echo=True)
    
    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    print("✅ All tables created successfully!")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(create_tables())