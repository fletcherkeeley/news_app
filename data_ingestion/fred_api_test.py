import requests
import json
from datetime import datetime

# FRED API Configuration
# The FRED API base URL - all requests start with this
API_BASE = "https://api.stlouisfed.org/fred"
# Your personal API key - this authenticates you with FRED
API_KEY = "f418c0623911721d7400fc124662758c"

def test_fred_connection():
    """
    Test basic connectivity to FRED API by fetching GDP series information
    
    Why GDP? It's one of the most important economic indicators:
    - Series ID: "GDP" (Gross Domestic Product)  
    - Updated quarterly
    - Long historical data available
    - Good representative example
    """
    
    print("🔍 Testing FRED API Connection...")
    print(f"📡 API Base URL: {API_BASE}")
    print(f"🔑 Using API Key: {API_KEY[:8]}..." + "*" * 24)  # Hide most of the key for security
    print("-" * 50)

    # STEP 1: Get series information (metadata about GDP series)
    print("\n📊 STEP 1: Fetching GDP series metadata...")
    
    # Build the URL for series information
    # /series endpoint gives us metadata about a specific series
    series_url = f"{API_BASE}/series"
    
    # Parameters that FRED API expects
    series_params = {
        'series_id': 'GDP',           # The specific series we want (GDP)
        'api_key': API_KEY,           # Your authentication key
        'file_type': 'json'           # We want JSON response (not XML)
    }
    
    try:
        # Make the HTTP GET request to FRED
        print(f"🌐 Making request to: {series_url}")
        print(f"📝 Parameters: {series_params}")
        
        response = requests.get(series_url, params=series_params)
        
        # Check if request was successful (status code 200 = OK)
        print(f"📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            # Parse the JSON response
            series_data = response.json()
            
            print("✅ Series metadata retrieved successfully!")
            print(f"📄 Raw response keys: {list(series_data.keys())}")
            
            # FRED wraps data in a 'seriess' array (note the double 's')
            if 'seriess' in series_data and len(series_data['seriess']) > 0:
                series_info = series_data['seriess'][0]  # Get first (and only) series
                
                print(f"\n📈 GDP Series Information:")
                print(f"   Title: {series_info['title']}")
                print(f"   Frequency: {series_info['frequency']}")
                print(f"   Units: {series_info['units']}")
                print(f"   Last Updated: {series_info['last_updated']}")
                print(f"   Observation Start: {series_info['observation_start']}")
                print(f"   Observation End: {series_info['observation_end']}")
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            return
            
    except Exception as e:
        print(f"❌ Error fetching series info: {e}")
        return

    # STEP 2: Get actual observations (data points) for GDP
    print(f"\n📊 STEP 2: Fetching recent GDP observations...")
    
    # Build URL for observations (actual data points)
    observations_url = f"{API_BASE}/series/observations"
    
    # Parameters for getting recent data points
    obs_params = {
        'series_id': 'GDP',           # Same series (GDP)
        'api_key': API_KEY,           # Your authentication key
        'file_type': 'json',          # JSON response
        'limit': 5,                   # Only get last 5 data points (for testing)
        'sort_order': 'desc'          # Most recent first
    }
    
    try:
        print(f"🌐 Making request to: {observations_url}")
        print(f"📝 Parameters: {obs_params}")
        
        response = requests.get(observations_url, params=obs_params)
        print(f"📡 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            obs_data = response.json()
            
            print("✅ Observations retrieved successfully!")
            print(f"📄 Raw response keys: {list(obs_data.keys())}")
            
            # FRED wraps observations in 'observations' array
            if 'observations' in obs_data:
                observations = obs_data['observations']
                print(f"\n📈 Found {len(observations)} recent GDP data points:")
                
                # Print each observation
                for i, obs in enumerate(observations, 1):
                    print(f"   {i}. Date: {obs['date']}, Value: {obs['value']}")
                    
                # Show the complete structure of one observation
                if observations:
                    print(f"\n🔍 Complete structure of first observation:")
                    print(json.dumps(observations[0], indent=2))
                    
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error fetching observations: {e}")

    print(f"\n✅ FRED API test completed at {datetime.now()}")

# Run the test when script is executed
if __name__ == "__main__":
    test_fred_connection()