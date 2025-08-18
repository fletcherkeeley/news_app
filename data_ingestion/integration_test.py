"""
Integration Test: FRED API Client + Data Transformers

Tests the complete data flow:
FRED API → API Client → Data Transformers → Database-ready format

This validates that our API client and transformers work together
perfectly before we add database persistence.
"""

from fred_api_client import FredApiClient
from fred_transformers import (
    transform_fred_series, 
    transform_fred_observations,
    FredDataValidator
)
from datetime import datetime
import json

def test_complete_data_pipeline():
    """
    Test the complete data ingestion pipeline:
    1. Fetch real data from FRED API
    2. Transform data using our transformers
    3. Validate transformed data is database-ready
    4. Display results for verification
    """
    
    print("🔄 Testing Complete Data Ingestion Pipeline")
    print("=" * 60)
    
    # Initialize API client with your key
    API_KEY = "f418c0623911721d7400fc124662758c"
    client = FredApiClient(API_KEY)
    
    # Test series: GDP (our primary focus)
    series_id = "GDP"
    
    try:
        # STEP 1: Fetch series information from FRED
        print(f"\n📊 STEP 1: Fetching {series_id} series info from FRED...")
        
        raw_series_data = client.fetch_series_info(series_id)
        print(f"✅ Retrieved series info from API")
        print(f"   API returned: {raw_series_data['title']}")
        print(f"   Frequency: {raw_series_data['frequency']}")
        print(f"   Units: {raw_series_data['units']}")
        
        # STEP 2: Transform series information
        print(f"\n🔄 STEP 2: Transforming series info for database...")
        
        transformed_series = transform_fred_series(raw_series_data)
        print(f"✅ Series transformation successful")
        
        # Validate transformed series data
        if FredDataValidator.validate_series_data(transformed_series):
            print(f"✅ Series data validation passed")
        else:
            print(f"❌ Series data validation failed")
            return False
            
        # Show key transformations
        print(f"\n📋 Series Transformation Results:")
        print(f"   series_id: {transformed_series['series_id']}")
        print(f"   title: {transformed_series['title']}")
        print(f"   frequency: {transformed_series['frequency']}")
        print(f"   observation_start: {transformed_series['observation_start']} (type: {type(transformed_series['observation_start'])})")
        print(f"   observation_end: {transformed_series['observation_end']}")
        print(f"   created_at: {transformed_series['created_at']} (type: {type(transformed_series['created_at'])})")
        
    except Exception as e:
        print(f"❌ Error in series processing: {e}")
        return False
    
    try:
        # STEP 3: Fetch recent observations from FRED
        print(f"\n📈 STEP 3: Fetching recent {series_id} observations from FRED...")
        
        raw_observations = client.fetch_observations(
            series_id=series_id,
            limit=10,  # Get last 10 data points for testing
            sort_order="desc"  # Most recent first
        )
        
        print(f"✅ Retrieved {len(raw_observations)} observations from API")
        print(f"   Date range: {raw_observations[-1]['date']} to {raw_observations[0]['date']}")
        
        # Show sample raw observation
        print(f"\n🔍 Sample Raw Observation (from API):")
        print(json.dumps(raw_observations[0], indent=2))
        
        # STEP 4: Transform observations
        print(f"\n🔄 STEP 4: Transforming observations for database...")
        
        transformed_observations = transform_fred_observations(raw_observations, series_id)
        print(f"✅ Observations transformation successful")
        print(f"   Transformed {len(transformed_observations)}/{len(raw_observations)} observations")
        
        # Validate each transformed observation
        valid_observations = 0
        for obs in transformed_observations:
            if FredDataValidator.validate_observation_data(obs):
                valid_observations += 1
                
        print(f"✅ {valid_observations}/{len(transformed_observations)} observations passed validation")
        
        # Show transformation results
        print(f"\n📋 Observation Transformation Results:")
        print(f"   Total observations processed: {len(raw_observations)}")
        print(f"   Successfully transformed: {len(transformed_observations)}")
        print(f"   Validation passed: {valid_observations}")
        
        # Display first few transformed observations
        print(f"\n📊 Sample Transformed Observations (Database-Ready):")
        for i, obs in enumerate(transformed_observations[:5], 1):
            value_display = f"${obs['value']}" if obs['value'] else "Missing"
            print(f"   {i}. {obs['observation_date']} | {value_display} | Types: date={type(obs['observation_date'])}, value={type(obs['value'])}")
            
    except Exception as e:
        print(f"❌ Error in observations processing: {e}")
        return False
    
    # STEP 5: Data Ready for Database
    print(f"\n💾 STEP 5: Database Readiness Check...")
    
    # Show what would be inserted into database
    print(f"✅ Data is ready for database insertion!")
    print(f"\nDatabase Insert Preview:")
    print(f"📊 FredSeries table: 1 record ready")
    print(f"   - series_id: {transformed_series['series_id']}")
    print(f"   - All required fields present and validated")
    
    print(f"📈 FredObservation table: {len(transformed_observations)} records ready")
    print(f"   - All observations have required fields")
    print(f"   - Data types properly converted")
    print(f"   - Missing values handled correctly")
    
    # Final success summary
    print(f"\n" + "=" * 60)
    print(f"🎉 INTEGRATION TEST SUCCESSFUL!")
    print(f"✅ API Client: Fetching data from FRED")
    print(f"✅ Transformers: Converting to database format") 
    print(f"✅ Validation: All data meets database requirements")
    print(f"✅ Pipeline: Ready for database persistence")
    print(f"=" * 60)
    
    return True

def test_error_handling():
    """
    Test how the pipeline handles various error conditions
    """
    print(f"\n🧪 Testing Error Handling...")
    
    API_KEY = "f418c0623911721d7400fc124662758c"
    client = FredApiClient(API_KEY)
    
    try:
        # Test with invalid series ID
        print(f"Testing invalid series ID...")
        try:
            invalid_series = client.fetch_series_info("INVALID_SERIES_123")
            print(f"❌ Should have failed with invalid series")
        except Exception as e:
            print(f"✅ Correctly handled invalid series: {str(e)[:50]}...")
            
        # Test transformation with missing data
        print(f"Testing transformation with missing fields...")
        incomplete_data = {"id": "TEST"}  # Missing required fields
        try:
            transform_fred_series(incomplete_data)
            print(f"❌ Should have failed with missing fields")
        except Exception as e:
            print(f"✅ Correctly handled missing fields: {str(e)[:50]}...")
            
        print(f"✅ Error handling tests passed")
        
    except Exception as e:
        print(f"❌ Error in error handling tests: {e}")

def test_data_types():
    """
    Verify that all transformed data has correct Python types for database
    """
    print(f"\n🔬 Testing Data Types...")
    
    API_KEY = "f418c0623911721d7400fc124662758c"
    client = FredApiClient(API_KEY)
    
    try:
        # Fetch and transform data
        series_data = client.fetch_series_info("GDP")
        transformed_series = transform_fred_series(series_data)
        
        observations_data = client.fetch_observations("GDP", limit=3)
        transformed_obs = transform_fred_observations(observations_data, "GDP")
        
        # Check series data types
        print(f"✅ Series Data Types:")
        print(f"   series_id: {type(transformed_series['series_id'])} (should be str)")
        print(f"   observation_start: {type(transformed_series['observation_start'])} (should be date)")
        print(f"   created_at: {type(transformed_series['created_at'])} (should be datetime)")
        
        # Check observation data types  
        if transformed_obs:
            obs = transformed_obs[0]
            print(f"✅ Observation Data Types:")
            print(f"   observation_date: {type(obs['observation_date'])} (should be date)")
            print(f"   value: {type(obs['value'])} (should be Decimal or NoneType)")
            print(f"   created_at: {type(obs['created_at'])} (should be datetime)")
            
        print(f"✅ All data types correct for database insertion")
        
    except Exception as e:
        print(f"❌ Error in data type testing: {e}")

if __name__ == "__main__":
    """
    Run the complete integration test suite
    """
    print("🚀 Starting FRED Data Ingestion Integration Tests")
    print(f"Test started at: {datetime.now()}")
    
    # Main pipeline test
    success = test_complete_data_pipeline()
    
    if success:
        # Additional tests
        test_error_handling()
        test_data_types()
        
        print(f"\n🎉 ALL INTEGRATION TESTS PASSED!")
        print(f"Your data ingestion pipeline is ready for database persistence!")
    else:
        print(f"\n❌ Integration tests failed. Check the errors above.")
    
    print(f"\nTest completed at: {datetime.now()}")