"""
FRED Data Ingestion Orchestration Service

Coordinates the complete data ingestion workflow:
1. Fetch data from FRED API using FredApiClient
2. Transform data using FredTransformers
3. Validate data using FredDataValidator
4. Save to PostgreSQL database using SQLAlchemy

Handles error recovery, logging, and batch processing.
This is the main service that automates your economic data collection.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta, UTC
from typing import List, Dict, Optional, Tuple
from contextlib import asynccontextmanager

# Database imports
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

# Our custom modules
from fred_api_client import FredApiClient
from fred_transformers import (
    transform_fred_series, 
    transform_fred_observations,
    FredDataValidator,
    FredTransformationError
)
from database_setup import FredSeries, FredObservation, FredSyncLog

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FredIngestionService:
    """
    Orchestrates the complete FRED data ingestion process
    
    Manages:
    - API client for data fetching
    - Database connections for data persistence  
    - Error handling and retry logic
    - Sync logging and monitoring
    - Batch processing of multiple series
    """
    
    def __init__(self, api_key: str, database_url: str):
        """
        Initialize the ingestion service
        
        Args:
            api_key: FRED API key for authentication
            database_url: PostgreSQL connection string
        """
        self.api_key = api_key
        self.database_url = database_url
        
        # Initialize API client
        self.fred_client = FredApiClient(api_key)
        
        # Initialize database engine and session factory
        self.engine = create_async_engine(
            database_url,
            echo=False,  # Set to True for SQL query logging
            pool_size=5,
            max_overflow=10
        )
        
        # Create async session factory
        self.AsyncSessionLocal = sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info("FRED Ingestion Service initialized")
    
    @asynccontextmanager
    async def get_db_session(self):
        """
        Async context manager for database sessions
        Ensures proper cleanup even if errors occur
        """
        async with self.AsyncSessionLocal() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}")
                raise
            finally:
                await session.close()
    
    async def ingest_series(
        self, 
        series_id: str = "GDP",
        update_existing: bool = False,
        days_lookback: int = 30
    ) -> Dict:
        """
        Ingest a single economic series from FRED into the database
        
        Args:
            series_id: FRED series identifier (e.g., "GDP", "UNRATE")
            update_existing: Whether to update existing series info
            days_lookback: How many days back to fetch new observations
            
        Returns:
            Dictionary with sync results:
            - success: bool
            - series_added: bool  
            - observations_added: int
            - observations_updated: int
            - error_message: str (if error occurred)
        """
        
        sync_start_time = datetime.now(UTC)
        result = {
            'series_id': series_id,
            'success': False,
            'series_added': False,
            'observations_added': 0,
            'observations_updated': 0,
            'error_message': None,
            'api_calls_used': 0
        }
        
        logger.info(f"Starting ingestion for series: {series_id}")
        
        try:
            # STEP 1: Fetch and process series information
            logger.info(f"Fetching series info for {series_id}")
            raw_series_data = self.fred_client.fetch_series_info(series_id)
            result['api_calls_used'] += 1
            
            # Transform series data
            transformed_series = transform_fred_series(raw_series_data)
            
            # Validate series data
            if not FredDataValidator.validate_series_data(transformed_series):
                raise FredTransformationError(f"Series data validation failed for {series_id}")
            
            # STEP 2: Save or update series in database
            async with self.get_db_session() as session:
                series_added = await self._upsert_series(session, transformed_series, update_existing)
                result['series_added'] = series_added
                
                logger.info(f"Series {'added' if series_added else 'updated'}: {series_id}")
            
            # STEP 3: Determine observation date range to fetch
            start_date = None
            if not update_existing:
                # For new series, get all available data
                start_date = None
            else:
                # For existing series, only get recent data
                cutoff_date = datetime.now().date() - timedelta(days=days_lookback)
                start_date = cutoff_date
            
            # STEP 4: Fetch observations
            logger.info(f"Fetching observations for {series_id} from {start_date or 'beginning'}")
            raw_observations = self.fred_client.fetch_observations(
                series_id=series_id,
                start_date=start_date,
                sort_order="asc"  # Chronological order for database insertion
            )
            result['api_calls_used'] += 1
            
            if not raw_observations:
                logger.warning(f"No observations found for {series_id}")
                result['success'] = True  # Not an error, just no new data
                return result
            
            # STEP 5: Transform observations
            logger.info(f"Transforming {len(raw_observations)} observations for {series_id}")
            transformed_observations = transform_fred_observations(raw_observations, series_id)
            
            # Validate observations
            valid_observations = []
            for obs in transformed_observations:
                if FredDataValidator.validate_observation_data(obs):
                    valid_observations.append(obs)
                else:
                    logger.warning(f"Invalid observation skipped for {series_id}: {obs.get('observation_date')}")
            
            logger.info(f"Validated {len(valid_observations)}/{len(transformed_observations)} observations")
            
            # STEP 6: Save observations to database
            if valid_observations:
                async with self.get_db_session() as session:
                    added, updated = await self._upsert_observations(session, valid_observations)
                    result['observations_added'] = added
                    result['observations_updated'] = updated
                    
                    logger.info(f"Saved observations: {added} added, {updated} updated")
            
            # STEP 7: Log successful sync
            result['success'] = True
            await self._log_sync_result(result, sync_start_time)
            
            logger.info(f"✅ Successfully ingested {series_id}: "
                       f"{result['observations_added']} new observations")
            
        except Exception as e:
            # Log the error
            result['error_message'] = str(e)
            logger.error(f"❌ Error ingesting {series_id}: {e}")
            
            # Log failed sync
            await self._log_sync_result(result, sync_start_time)
        
        return result
    
    async def _upsert_series(
        self, 
        session: AsyncSession, 
        series_data: Dict, 
        update_existing: bool
    ) -> bool:
        """
        Insert or update series information in database
        
        Returns:
            True if new series was added, False if existing series was updated
        """
        series_id = series_data['series_id']
        
        # Check if series already exists
        existing_series = await session.get(FredSeries, series_id)
        
        if existing_series and not update_existing:
            logger.info(f"Series {series_id} already exists, skipping update")
            return False
        
        if existing_series:
            # Update existing series
            for key, value in series_data.items():
                if key != 'created_at':  # Don't update creation timestamp
                    setattr(existing_series, key, value)
            logger.info(f"Updated existing series: {series_id}")
            return False
        else:
            # Insert new series
            new_series = FredSeries(**series_data)
            session.add(new_series)
            logger.info(f"Added new series: {series_id}")
            return True
    
    async def _upsert_observations(
        self, 
        session: AsyncSession, 
        observations: List[Dict]
    ) -> Tuple[int, int]:
        """
        Insert or update observations in database using manual check-then-insert logic
        (Avoiding ON CONFLICT due to constraint visibility issues)
        
        Returns:
            Tuple of (observations_added, observations_updated)
        """
        if not observations:
            return 0, 0
        
        series_id = observations[0]['series_id']
        observations_added = 0
        observations_updated = 0
        
        for obs_data in observations:
            # Check if observation already exists
            # Check if observation already exists
            stmt = select(FredObservation).where(
                and_(
                    FredObservation.series_id == obs_data['series_id'],
                    FredObservation.observation_date == obs_data['observation_date']
                )
            )
            result = await session.execute(stmt)
            existing_obs = result.scalar_one_or_none()
            
            if existing_obs:
                # Update existing observation
                existing_obs.value = obs_data['value']
                existing_obs.realtime_start = obs_data['realtime_start']
                existing_obs.realtime_end = obs_data['realtime_end']
                observations_updated += 1
                logger.debug(f"Updated observation: {series_id} on {obs_data['observation_date']}")
            else:
                # Insert new observation
                new_obs = FredObservation(**obs_data)
                session.add(new_obs)
                observations_added += 1
                logger.debug(f"Added new observation: {series_id} on {obs_data['observation_date']}")
        
        logger.info(f"Processed {len(observations)} observations for {series_id}: "
                   f"{observations_added} added, {observations_updated} updated")
        return observations_added, observations_updated
    
    async def _log_sync_result(self, result: Dict, sync_start_time: datetime):
        """Log the sync operation to FredSyncLog table"""
        try:
            async with self.get_db_session() as session:
                log_entry = FredSyncLog(
                    series_id=result['series_id'],
                    sync_date=sync_start_time,
                    records_added=result['observations_added'],
                    records_updated=result['observations_updated'],
                    success=result['success'],
                    error_message=result['error_message'],
                    api_calls_used=result['api_calls_used']
                )
                session.add(log_entry)
        except Exception as e:
            logger.error(f"Error logging sync result: {e}")
    
    async def sync_multiple_series(
        self, 
        series_ids: List[str],
        update_existing: bool = True,
        days_lookback: int = 30
    ) -> Dict:
        """
        Ingest multiple economic series efficiently
        
        Args:
            series_ids: List of FRED series identifiers
            update_existing: Whether to update existing series
            days_lookback: How many days back to fetch new observations
            
        Returns:
            Summary of all sync operations
        """
        logger.info(f"Starting batch sync for {len(series_ids)} series")
        
        results = []
        total_api_calls = 0
        successful_series = 0
        failed_series = 0
        
        for series_id in series_ids:
            try:
                result = await self.ingest_series(
                    series_id=series_id,
                    update_existing=update_existing,
                    days_lookback=days_lookback
                )
                results.append(result)
                total_api_calls += result['api_calls_used']
                
                if result['success']:
                    successful_series += 1
                else:
                    failed_series += 1
                    
                # Brief pause between series to be respectful to FRED API
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Unexpected error processing {series_id}: {e}")
                failed_series += 1
        
        summary = {
            'total_series': len(series_ids),
            'successful_series': successful_series,
            'failed_series': failed_series,
            'total_api_calls': total_api_calls,
            'sync_timestamp': datetime.now(UTC),
            'detailed_results': results
        }
        
        logger.info(f"✅ Batch sync completed: {successful_series}/{len(series_ids)} successful")
        return summary
    
    async def get_sync_status(self, series_id: str) -> Optional[Dict]:
        """Get the last sync status for a series"""
        try:
            async with self.get_db_session() as session:
                # Get the most recent sync log entry
                stmt = select(FredSyncLog).where(
                    FredSyncLog.series_id == series_id
                ).order_by(FredSyncLog.sync_date.desc()).limit(1)
                
                result = await session.execute(stmt)
                sync_log = result.scalar_one_or_none()
                
                if sync_log:
                    return {
                        'series_id': sync_log.series_id,
                        'last_sync': sync_log.sync_date,
                        'success': sync_log.success,
                        'records_added': sync_log.records_added,
                        'records_updated': sync_log.records_updated,
                        'error_message': sync_log.error_message,
                        'api_calls_used': sync_log.api_calls_used
                    }
                
                return None
        except Exception as e:
            logger.error(f"Error getting sync status for {series_id}: {e}")
            return None
    
    async def close(self):
        """Clean up database connections"""
        await self.engine.dispose()
        logger.info("FRED Ingestion Service closed")


# Convenience functions for easy usage
async def create_ingestion_service(api_key: str, database_url: str) -> FredIngestionService:
    """Factory function to create and initialize the ingestion service"""
    service = FredIngestionService(api_key, database_url)
    return service

async def ingest_gdp_data(api_key: str, database_url: str) -> Dict:
    """Convenience function to ingest GDP data specifically"""
    service = await create_ingestion_service(api_key, database_url)
    try:
        result = await service.ingest_series("GDP")
        return result
    finally:
        await service.close()


# Example usage and testing
if __name__ == "__main__":
    """
    Test the complete ingestion service with GDP data
    """
    
    async def test_ingestion_service():
        # Configuration
        API_KEY = "f418c0623911721d7400fc124662758c"
        DATABASE_URL = "postgresql+psycopg://postgres:fred_password@localhost:5432/postgres"
        
        print("🚀 Testing FRED Ingestion Service...")
        print(f"Started at: {datetime.now()}")
        
        # Create service
        service = await create_ingestion_service(API_KEY, DATABASE_URL)
        
        try:
            # Test single series ingestion
            print(f"\n📊 Testing GDP data ingestion...")
            result = await service.ingest_series("GDP", update_existing=True, days_lookback=90)
            
            if result['success']:
                print(f"✅ GDP ingestion successful!")
                print(f"   Series {'added' if result['series_added'] else 'updated'}")
                print(f"   Observations added: {result['observations_added']}")
                print(f"   Observations updated: {result['observations_updated']}")
                print(f"   API calls used: {result['api_calls_used']}")
            else:
                print(f"❌ GDP ingestion failed: {result['error_message']}")
            
            # Test sync status
            print(f"\n📋 Checking sync status...")
            status = await service.get_sync_status("GDP")
            if status:
                print(f"✅ Last sync: {status['last_sync']}")
                print(f"   Success: {status['success']}")
                print(f"   Records processed: {status['records_added'] + status['records_updated']}")
            
            # Test multiple series (if you want to expand)
            print(f"\n📈 Testing multiple series sync...")
            multiple_result = await service.sync_multiple_series(
                series_ids=["GDP", "UNRATE", "CPIAUCSL", "FEDFUNDS", "PAYEMS"],  # GDP + Unemployment Rate + Consumer Price Index + Federal Funds Rate + total Nonfarm Payrolls
                update_existing=False,
                days_lookback=30
            )
            
            print(f"✅ Multiple series sync completed!")
            print(f"   Successful: {multiple_result['successful_series']}/{multiple_result['total_series']}")
            print(f"   Total API calls: {multiple_result['total_api_calls']}")
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            
        finally:
            await service.close()
        
        print(f"\n🎉 Testing completed at: {datetime.now()}")
    
    # Run the async test
    asyncio.run(test_ingestion_service())