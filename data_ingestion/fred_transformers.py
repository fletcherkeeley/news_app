from datetime import datetime, date, UTC
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any
import logging

# Set up logging for transformation issues
logger = logging.getLogger(__name__)

class FredTransformationError(Exception):
    """Custom exception for FRED data transformation errors"""
    pass

class FredSeriesTransformer:
    """
    Transforms FRED API series metadata into database-ready format
    
    Handles conversion from FRED API response format to FredSeries table schema
    """
    
    @staticmethod
    def transform_series_info(fred_series_data: Dict) -> Dict:
        """
        Transform FRED series metadata to match FredSeries database table
        
        Args:
            fred_series_data: Raw series data from FRED API (from 'seriess' array)
            
        Returns:
            Dictionary matching FredSeries table columns
            
        Example:
            fred_data = {
                "id": "GDP",
                "title": "Gross Domestic Product", 
                "frequency": "Quarterly",
                "units": "Billions of Dollars",
                "observation_start": "1947-01-01",
                "observation_end": "2025-04-01",
                "last_updated": "2025-07-30 07:56:35-05",
                "notes": "BEA Account Code: A191RL..."
            }
            
            transformed = FredSeriesTransformer.transform_series_info(fred_data)
            # Result matches FredSeries table structure
        """
        try:
            # Extract and validate required fields
            series_id = fred_series_data.get('id')
            if not series_id:
                raise FredTransformationError("Missing required field: 'id' (series_id)")
            
            title = fred_series_data.get('title', '').strip()
            if not title:
                raise FredTransformationError(f"Missing or empty title for series {series_id}")
            
            # Transform the data to match our database schema
            transformed_data = {
                # Direct mappings
                'series_id': series_id,
                'title': title[:500],  # Truncate to database limit
                'units': fred_series_data.get('units', '')[:200],  # Truncate to limit
                'units_short': fred_series_data.get('units_short', '')[:50],
                'frequency': fred_series_data.get('frequency', '')[:20],
                'frequency_short': fred_series_data.get('frequency_short', '')[:5],
                'seasonal_adjustment': fred_series_data.get('seasonal_adjustment', '')[:50],
                'source': fred_series_data.get('source', '')[:200],
                'notes': fred_series_data.get('notes', ''),
                'popularity': fred_series_data.get('popularity', 0),
                
                # Date transformations
                'observation_start': FredSeriesTransformer._parse_date(
                    fred_series_data.get('observation_start')
                ),
                'observation_end': FredSeriesTransformer._parse_date(
                    fred_series_data.get('observation_end')
                ),
                'last_updated': FredSeriesTransformer._parse_datetime(
                    fred_series_data.get('last_updated')
                ),
                
                # Auto-generated fields
                'created_at': datetime.now(UTC),
                'updated_at': datetime.now(UTC)
            }
            
            # Extract category information if available
            # FRED sometimes includes category info in the series data
            if 'category_id' in fred_series_data:
                transformed_data['category'] = str(fred_series_data['category_id'])
            
            logger.info(f"Successfully transformed series: {series_id}")
            return transformed_data
            
        except Exception as e:
            error_msg = f"Error transforming series data: {e}"
            logger.error(error_msg)
            raise FredTransformationError(error_msg) from e
    
    @staticmethod
    def _parse_date(date_string: Optional[str]) -> Optional[date]:
        """
        Parse FRED date string to Python date object
        
        Args:
            date_string: Date in YYYY-MM-DD format or None
            
        Returns:
            date object or None if parsing fails
        """
        if not date_string:
            return None
            
        try:
            # FRED dates are typically in YYYY-MM-DD format
            return datetime.strptime(date_string, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Could not parse date: {date_string}")
            return None
    
    @staticmethod
    def _parse_datetime(datetime_string: Optional[str]) -> Optional[datetime]:
        """
        Parse FRED datetime string to Python datetime object
        
        Args:
            datetime_string: Datetime string from FRED (various formats possible)
            
        Returns:
            datetime object or None if parsing fails
        """
        if not datetime_string:
            return None
            
        try:
            # Try common FRED datetime formats
            # Format: "2025-07-30 07:56:35-05"
            if ' ' in datetime_string and '-' in datetime_string:
                # Remove timezone info for simplicity (take first part before timezone)
                dt_part = datetime_string.split('-')[:-1]  # Remove timezone
                dt_string = '-'.join(dt_part)
                return datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
            
            # Format: "2025-07-30"
            return datetime.strptime(datetime_string, '%Y-%m-%d')
            
        except ValueError:
            logger.warning(f"Could not parse datetime: {datetime_string}")
            return None


class FredObservationTransformer:
    """
    Transforms FRED API observation data into database-ready format
    
    Handles conversion from FRED observation format to FredObservation table schema
    """
    
    @staticmethod
    def transform_observations(
        fred_observations: List[Dict], 
        series_id: str
    ) -> List[Dict]:
        """
        Transform a list of FRED observations to match FredObservation database table
        
        Args:
            fred_observations: List of observation dicts from FRED API
            series_id: The series these observations belong to
            
        Returns:
            List of dictionaries matching FredObservation table columns
            
        Example:
            fred_obs = [
                {
                    "date": "2025-04-01",
                    "value": "30331.117", 
                    "realtime_start": "2025-08-07",
                    "realtime_end": "2025-08-07"
                }
            ]
            
            transformed = FredObservationTransformer.transform_observations(fred_obs, "GDP")
            # Result matches FredObservation table structure
        """
        transformed_observations = []
        
        for obs_data in fred_observations:
            try:
                transformed_obs = FredObservationTransformer._transform_single_observation(
                    obs_data, series_id
                )
                if transformed_obs:  # Only add if transformation was successful
                    transformed_observations.append(transformed_obs)
                    
            except Exception as e:
                logger.error(f"Error transforming observation for {series_id}: {e}")
                # Continue processing other observations even if one fails
                continue
        
        logger.info(f"Transformed {len(transformed_observations)}/{len(fred_observations)} observations for {series_id}")
        return transformed_observations
    
    @staticmethod
    def _transform_single_observation(obs_data: Dict, series_id: str) -> Optional[Dict]:
        """
        Transform a single FRED observation to database format
        
        Args:
            obs_data: Single observation dict from FRED API
            series_id: The series this observation belongs to
            
        Returns:
            Dictionary matching FredObservation table or None if invalid
        """
        # Validate required fields
        if 'date' not in obs_data:
            logger.warning(f"Observation missing date field for series {series_id}")
            return None
        
        # Parse observation date
        obs_date = FredObservationTransformer._parse_date(obs_data['date'])
        if not obs_date:
            logger.warning(f"Invalid date '{obs_data['date']}' for series {series_id}")
            return None
        
        # Parse observation value (handle missing data)
        obs_value = FredObservationTransformer._parse_value(obs_data.get('value'))
        
        # Parse realtime dates
        realtime_start = FredObservationTransformer._parse_date(
            obs_data.get('realtime_start')
        )
        realtime_end = FredObservationTransformer._parse_date(
            obs_data.get('realtime_end')
        )
        
        return {
            'series_id': series_id,
            'observation_date': obs_date,
            'value': obs_value,  # Can be None for missing data
            'realtime_start': realtime_start,
            'realtime_end': realtime_end,
            'created_at': datetime.now(UTC)
        }
    
    @staticmethod
    def _parse_date(date_string: Optional[str]) -> Optional[date]:
        """Parse FRED date string to Python date object"""
        if not date_string:
            return None
            
        try:
            return datetime.strptime(date_string, '%Y-%m-%d').date()
        except ValueError:
            logger.warning(f"Could not parse date: {date_string}")
            return None
    
    @staticmethod
    def _parse_value(value_string: Optional[str]) -> Optional[Decimal]:
        """
        Parse FRED observation value to Decimal
        
        Args:
            value_string: Value from FRED API (string number or "." for missing)
            
        Returns:
            Decimal value or None for missing/invalid data
            
        Note:
            FRED uses "." to represent missing data points
        """
        if not value_string or value_string == ".":
            # FRED uses "." for missing data - this is normal, not an error
            return None
        
        try:
            # Convert string to Decimal for precise financial calculations
            return Decimal(str(value_string))
        except (InvalidOperation, ValueError):
            logger.warning(f"Could not parse value: '{value_string}'")
            return None


class FredDataValidator:
    """
    Validates transformed FRED data before database insertion
    """
    
    @staticmethod
    def validate_series_data(series_data: Dict) -> bool:
        """
        Validate transformed series data meets database requirements
        
        Args:
            series_data: Transformed series data dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['series_id', 'title']
        
        for field in required_fields:
            if field not in series_data or not series_data[field]:
                logger.error(f"Missing required series field: {field}")
                return False
        
        # Validate series_id length (database limit: 50 chars)
        if len(series_data['series_id']) > 50:
            logger.error(f"Series ID too long: {series_data['series_id']}")
            return False
        
        return True
    
    @staticmethod
    def validate_observation_data(obs_data: Dict) -> bool:
        """
        Validate transformed observation data meets database requirements
        
        Args:
            obs_data: Transformed observation data dictionary
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['series_id', 'observation_date']
        
        for field in required_fields:
            if field not in obs_data or obs_data[field] is None:
                logger.error(f"Missing required observation field: {field}")
                return False
        
        return True


# Convenience functions for easy usage
def transform_fred_series(fred_series_data: Dict) -> Dict:
    """Convenience function to transform series data"""
    return FredSeriesTransformer.transform_series_info(fred_series_data)

def transform_fred_observations(fred_observations: List[Dict], series_id: str) -> List[Dict]:
    """Convenience function to transform observations data"""
    return FredObservationTransformer.transform_observations(fred_observations, series_id)


# Example usage and testing
if __name__ == "__main__":
    # Test data transformation with sample FRED data
    print("🔄 Testing FRED Data Transformers...")
    
    # Sample FRED series data (like what comes from API)
    sample_series = {
        "id": "GDP",
        "title": "Gross Domestic Product",
        "frequency": "Quarterly", 
        "units": "Billions of Dollars",
        "units_short": "Bil. of $",
        "observation_start": "1947-01-01",
        "observation_end": "2025-04-01",
        "last_updated": "2025-07-30 07:56:35-05",
        "notes": "BEA Account Code: A191RL1Q225SBEA"
    }
    
    # Sample FRED observations data
    sample_observations = [
        {
            "date": "2025-04-01",
            "value": "30331.117",
            "realtime_start": "2025-08-07", 
            "realtime_end": "2025-08-07"
        },
        {
            "date": "2025-01-01", 
            "value": "29962.047",
            "realtime_start": "2025-08-07",
            "realtime_end": "2025-08-07"
        },
        {
            "date": "2024-10-01",
            "value": ".",  # Missing data test
            "realtime_start": "2025-08-07",
            "realtime_end": "2025-08-07"
        }
    ]
    
    try:
        # Test series transformation
        print("\n📊 Testing series transformation...")
        transformed_series = transform_fred_series(sample_series)
        print(f"✅ Series transformed: {transformed_series['series_id']}")
        print(f"   Title: {transformed_series['title']}")
        print(f"   Observation start: {transformed_series['observation_start']}")
        
        # Test observations transformation  
        print("\n📈 Testing observations transformation...")
        transformed_obs = transform_fred_observations(sample_observations, "GDP")
        print(f"✅ Transformed {len(transformed_obs)} observations")
        
        for i, obs in enumerate(transformed_obs, 1):
            value_display = f"${obs['value']}" if obs['value'] else "Missing"
            print(f"   {i}. {obs['observation_date']}: {value_display}")
            
        print(f"\n✅ All transformation tests passed!")
        
    except Exception as e:
        print(f"❌ Transformation test failed: {e}")