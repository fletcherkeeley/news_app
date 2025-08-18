import requests
import time
from datetime import datetime, date
from typing import Dict, List, Optional, Union

class FredApiClient:
    """
    Client for interacting with the Federal Reserve Economic Data (FRED) API
    
    Handles:
    - Authentication with API key
    - Rate limiting and error handling
    - Fetching series metadata and observations
    - Currently optimized for GDP data, easily extensible for other series
    """
    
    def __init__(self, api_key: str):
        """
        Initialize the FRED API client
        
        Args:
            api_key: Your FRED API key from https://fred.stlouisfed.org/docs/api/api_key.html
        """
        self.api_key = api_key
        self.base_url = "https://api.stlouisfed.org/fred"
        self.session = requests.Session()  # Reuse connections for efficiency
        
        # Rate limiting: FRED allows 120 requests per 60 seconds
        # We'll be conservative and track our request timing
        self.last_request_time = 0
        self.min_request_interval = 0.5  # Wait 500ms between requests
        
    def _make_request(self, endpoint: str, params: Dict) -> Dict:
        """
        Make a request to the FRED API with rate limiting and error handling
        
        Args:
            endpoint: API endpoint (e.g., 'series', 'series/observations')
            params: Dictionary of parameters to send
            
        Returns:
            Dictionary containing the JSON response from FRED
            
        Raises:
            Exception: If API request fails or returns error
        """
        # Rate limiting: ensure we don't make requests too quickly
        time_since_last_request = time.time() - self.last_request_time
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)
        
        # Build full URL
        url = f"{self.base_url}/{endpoint}"
        
        # Add API key and JSON format to all requests
        params.update({
            'api_key': self.api_key,
            'file_type': 'json'
        })
        
        try:
            # Make the HTTP request
            response = self.session.get(url, params=params)
            self.last_request_time = time.time()
            
            # Check for HTTP errors (4xx, 5xx status codes)
            if response.status_code != 200:
                raise Exception(f"FRED API error: {response.status_code} - {response.text}")
            
            # Parse JSON response
            data = response.json()
            
            # FRED sometimes returns errors in valid JSON format
            # Check if there's an error_code in the response
            if 'error_code' in data:
                raise Exception(f"FRED API error: {data['error_code']} - {data.get('error_message', 'Unknown error')}")
                
            return data
            
        except requests.RequestException as e:
            raise Exception(f"Network error connecting to FRED API: {e}")
        except ValueError as e:
            raise Exception(f"Error parsing JSON response from FRED API: {e}")
    
    def fetch_series_info(self, series_id: str = "GDP") -> Dict:
        """
        Fetch metadata about a FRED economic series
        
        Args:
            series_id: FRED series identifier (default: "GDP")
            
        Returns:
            Dictionary containing series information:
            - series_id, title, frequency, units, notes, etc.
            - observation_start, observation_end dates
            - last_updated timestamp
            
        Example:
            client = FredApiClient(api_key)
            gdp_info = client.fetch_series_info("GDP")
            print(gdp_info['title'])  # "Gross Domestic Product"
        """
        params = {'series_id': series_id}
        
        response_data = self._make_request('series', params)
        
        # FRED wraps series data in 'seriess' array (note double 's')
        if 'seriess' not in response_data or len(response_data['seriess']) == 0:
            raise Exception(f"No series data found for {series_id}")
            
        # Return the first (and should be only) series
        series_info = response_data['seriess'][0]
        
        return series_info
    
    def fetch_observations(
        self, 
        series_id: str = "GDP",
        limit: Optional[int] = None,
        start_date: Optional[Union[str, date]] = None,
        end_date: Optional[Union[str, date]] = None,
        sort_order: str = "asc"
    ) -> List[Dict]:
        """
        Fetch actual data points (observations) for a FRED series
        
        Args:
            series_id: FRED series identifier (default: "GDP")
            limit: Maximum number of observations to return (None for all)
            start_date: Start date for observations (YYYY-MM-DD format or date object)
            end_date: End date for observations (YYYY-MM-DD format or date object)  
            sort_order: "asc" for oldest first, "desc" for newest first
            
        Returns:
            List of dictionaries, each containing:
            - date: observation date (string in YYYY-MM-DD format)
            - value: observation value (string, may be "." for missing data)
            - realtime_start, realtime_end: when this value was valid
            
        Example:
            # Get last 10 GDP observations
            observations = client.fetch_observations("GDP", limit=10, sort_order="desc")
            for obs in observations:
                print(f"{obs['date']}: ${obs['value']} billion")
        """
        params = {'series_id': series_id, 'sort_order': sort_order}
        
        # Add optional parameters if provided
        if limit is not None:
            params['limit'] = limit
            
        if start_date is not None:
            # Convert date object to string if needed
            if isinstance(start_date, date):
                start_date = start_date.strftime('%Y-%m-%d')
            params['observation_start'] = start_date
            
        if end_date is not None:
            # Convert date object to string if needed
            if isinstance(end_date, date):
                end_date = end_date.strftime('%Y-%m-%d')
            params['observation_end'] = end_date
        
        response_data = self._make_request('series/observations', params)
        
        # FRED wraps observations in 'observations' array
        if 'observations' not in response_data:
            raise Exception(f"No observations data found for {series_id}")
            
        observations = response_data['observations']
        
        return observations
    
    def get_latest_observation(self, series_id: str = "GDP") -> Dict:
        """
        Convenience method to get the most recent data point for a series
        
        Args:
            series_id: FRED series identifier (default: "GDP")
            
        Returns:
            Dictionary containing the latest observation
            
        Example:
            latest_gdp = client.get_latest_observation("GDP")
            print(f"Latest GDP: ${latest_gdp['value']} billion as of {latest_gdp['date']}")
        """
        observations = self.fetch_observations(
            series_id=series_id, 
            limit=1, 
            sort_order="desc"
        )
        
        if not observations:
            raise Exception(f"No observations found for {series_id}")
            
        return observations[0]


# Convenience function for quick usage without creating a client object
def create_fred_client(api_key: str) -> FredApiClient:
    """
    Factory function to create a FRED API client
    
    Args:
        api_key: Your FRED API key
        
    Returns:
        Configured FredApiClient instance
    """
    return FredApiClient(api_key)


# Example usage and testing
if __name__ == "__main__":
    # Test the client with your API key
    API_KEY = "f418c0623911721d7400fc124662758c"
    
    # Create client
    client = create_fred_client(API_KEY)
    
    print("🔍 Testing FRED API Client...")
    
    try:
        # Test series info
        print("\n📊 Fetching GDP series info...")
        series_info = client.fetch_series_info("GDP")
        print(f"✅ {series_info['title']}")
        print(f"   Units: {series_info['units']}")
        print(f"   Frequency: {series_info['frequency']}")
        
        # Test recent observations
        print("\n📈 Fetching recent GDP observations...")
        recent_obs = client.fetch_observations("GDP", limit=3, sort_order="desc")
        for obs in recent_obs:
            print(f"   {obs['date']}: ${obs['value']} billion")
        
        # Test latest observation
        print("\n📊 Getting latest GDP value...")
        latest = client.get_latest_observation("GDP")
        print(f"✅ Latest GDP: ${latest['value']} billion ({latest['date']})")
        
        print(f"\n✅ All tests passed! Client is working correctly.")
        
    except Exception as e:
        print(f"❌ Error testing client: {e}")