"""
Database Migration: Add Unique Constraint for FRED Observations

This script adds a unique constraint on (series_id, observation_date) 
to the fred_observations table, which is required for PostgreSQL's 
ON CONFLICT upsert operations to work properly.

Why we need this:
- PostgreSQL ON CONFLICT requires a unique constraint to detect duplicates
- Ensures data integrity (one observation per series per date)
- Enables efficient upsert operations in our ingestion service
"""

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def add_unique_constraint():
    """
    Add unique constraint to fred_observations table
    
    Creates a unique constraint on (series_id, observation_date) which:
    1. Prevents duplicate observations for the same series on the same date
    2. Enables ON CONFLICT upsert operations in PostgreSQL
    3. Improves query performance on these commonly-used columns
    """
    
    # Database connection
    DATABASE_URL = "postgresql+psycopg://postgres:fred_password@localhost:5432/postgres"
    
    print("🔧 Adding unique constraint to fred_observations table...")
    print(f"📡 Connecting to: {DATABASE_URL.split('@')[1]}")  # Hide password in output
    
    try:
        # Create async engine
        engine = create_async_engine(DATABASE_URL, echo=True)
        
        async with engine.begin() as conn:
            
            # STEP 1: Check if constraint already exists
            print("\n🔍 Checking if unique constraint already exists...")
            
            check_constraint_sql = text("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'fred_observations' 
                AND constraint_type = 'UNIQUE'
                AND constraint_name LIKE '%series_id%observation_date%'
            """)
            
            result = await conn.execute(check_constraint_sql)
            existing_constraints = result.fetchall()
            
            if existing_constraints:
                print(f"✅ Unique constraint already exists:")
                for constraint in existing_constraints:
                    print(f"   - {constraint[0]}")
                print("No changes needed!")
                return
            
            # STEP 2: Check for existing duplicate data (would prevent constraint creation)
            print("\n🔍 Checking for existing duplicate observations...")
            
            duplicate_check_sql = text("""
                SELECT series_id, observation_date, COUNT(*) as duplicate_count
                FROM fred_observations
                GROUP BY series_id, observation_date
                HAVING COUNT(*) > 1
                ORDER BY duplicate_count DESC
                LIMIT 5
            """)
            
            result = await conn.execute(duplicate_check_sql)
            duplicates = result.fetchall()
            
            if duplicates:
                print(f"⚠️  Found {len(duplicates)} sets of duplicate observations:")
                for dup in duplicates:
                    print(f"   - {dup[0]} on {dup[1]}: {dup[2]} duplicates")
                
                print("\n🧹 Removing duplicates before adding constraint...")
                
                # Remove duplicates, keeping the most recent entry (highest id)
                deduplicate_sql = text("""
                    DELETE FROM fred_observations a 
                    USING fred_observations b 
                    WHERE a.series_id = b.series_id 
                    AND a.observation_date = b.observation_date 
                    AND a.id < b.id
                """)
                
                result = await conn.execute(deduplicate_sql)
                deleted_count = result.rowcount
                print(f"✅ Removed {deleted_count} duplicate observations")
            else:
                print("✅ No duplicate observations found")
            
            # STEP 3: Add the unique constraint
            print("\n🔧 Adding unique constraint...")
            
            constraint_name = "uk_fred_observations_series_date"
            add_constraint_sql = text(f"""
                ALTER TABLE fred_observations 
                ADD CONSTRAINT {constraint_name} 
                UNIQUE (series_id, observation_date)
            """)
            
            await conn.execute(add_constraint_sql)
            print(f"✅ Successfully added unique constraint: {constraint_name}")
            
            # STEP 4: Verify the constraint was created
            print("\n🔍 Verifying constraint creation...")
            
            verify_constraint_sql = text("""
                SELECT 
                    constraint_name,
                    constraint_type,
                    table_name
                FROM information_schema.table_constraints 
                WHERE table_name = 'fred_observations' 
                AND constraint_type = 'UNIQUE'
            """)
            
            result = await conn.execute(verify_constraint_sql)
            constraints = result.fetchall()
            
            if constraints:
                print(f"✅ Constraint verification successful:")
                for constraint in constraints:
                    print(f"   - {constraint[0]} ({constraint[1]})")
            else:
                print("❌ Constraint verification failed")
                
            # STEP 5: Test the constraint works
            print("\n🧪 Testing constraint functionality...")
            
            # Try to insert a duplicate (should fail)
            test_insert_sql = text("""
                INSERT INTO fred_observations (series_id, observation_date, value, created_at)
                VALUES ('TEST_SERIES', '2024-01-01', 100.0, NOW())
                ON CONFLICT (series_id, observation_date) 
                DO UPDATE SET value = EXCLUDED.value
                RETURNING series_id, observation_date, value
            """)
            
            result = await conn.execute(test_insert_sql)
            test_row = result.fetchone()
            
            if test_row:
                print(f"✅ ON CONFLICT upsert test successful!")
                print(f"   Upserted: {test_row[0]} on {test_row[1]} = {test_row[2]}")
                
                # Clean up test data
                cleanup_sql = text("DELETE FROM fred_observations WHERE series_id = 'TEST_SERIES'")
                await conn.execute(cleanup_sql)
                print(f"✅ Test data cleaned up")
            else:
                print("❌ ON CONFLICT test failed")
        
        # Close the engine
        await engine.dispose()
        
        print(f"\n🎉 Migration completed successfully!")
        print(f"✅ fred_observations table now has unique constraint on (series_id, observation_date)")
        print(f"✅ ON CONFLICT upsert operations will now work in the ingestion service")
        
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        logger.error(f"Migration failed: {e}")
        
        if "already exists" in str(e).lower():
            print("💡 Constraint might already exist - check manually if needed")
        else:
            print("💡 Check database connection and permissions")
            
        raise

async def check_constraint_status():
    """
    Check the current status of constraints on fred_observations table
    Useful for debugging and verification
    """
    DATABASE_URL = "postgresql+psycopg//postgres:fred_password@localhost:5432/postgres"
    
    try:
        engine = create_async_engine(DATABASE_URL, echo=False)
        
        async with engine.begin() as conn:
            print("📋 Current constraints on fred_observations table:")
            
            constraints_sql = text("""
                SELECT 
                    constraint_name,
                    constraint_type,
                    column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                WHERE tc.table_name = 'fred_observations'
                ORDER BY constraint_type, constraint_name
            """)
            
            result = await conn.execute(constraints_sql)
            constraints = result.fetchall()
            
            if constraints:
                current_type = None
                for constraint in constraints:
                    if constraint[1] != current_type:
                        print(f"\n{constraint[1]} Constraints:")
                        current_type = constraint[1]
                    print(f"   - {constraint[0]} on column '{constraint[2]}'")
            else:
                print("   No constraints found")
                
        await engine.dispose()
        
    except Exception as e:
        print(f"❌ Error checking constraints: {e}")

if __name__ == "__main__":
    """
    Run the migration to add unique constraint
    """
    
    print("🚀 FRED Database Migration: Adding Unique Constraint")
    print("=" * 60)
    
    # Run the migration
    asyncio.run(add_unique_constraint())
    
    print("\n" + "=" * 60)
    print("Migration completed! Your ingestion service should now work properly.")
    print("\n💡 Next steps:")
    print("   1. Run: python fred_ingestion_service.py")  
    print("   2. Verify data is successfully saved to database")
    print("   3. Check logs for any remaining issues")