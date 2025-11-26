#!/usr/bin/env python3
"""
Open-Meteo Temperature Data Extraction Script

This script extracts historical temperature data from Open-Meteo's Historical Weather API
for cities in the input CSV file and merges the data back.

Author: Generated for AG_Fisheries project
Date: 2025-11-26
"""

import pandas as pd
import requests
import time
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('temperature_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Open-Meteo API Configuration
OPENMETEO_URL = "https://archive-api.open-meteo.com/v1/archive"
RATE_LIMIT_DELAY = 0.2  # Increased from 0.1s to 0.2s to avoid rate limits (300 requests/minute)
MAX_RETRIES = 3
RETRY_DELAY = 2

class OpenMeteoTemperatureExtractor:
    """Extract temperature data from Open-Meteo Historical Weather API"""
    
    def __init__(self):
        """Initialize the extractor"""
        self.session = requests.Session()
        self.cache_file = Path('temperature_cache.json')
        self.cache = self._load_cache()
        
    def _load_cache(self) -> Dict:
        """Load cached temperature data to avoid redundant API calls"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load cache: {e}")
        return {}
    
    def _save_cache(self):
        """Save cache to disk"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save cache: {e}")
    
    def _get_cache_key(self, lat: float, lon: float, date: str) -> str:
        """Generate cache key for a location/date combination"""
        return f"{lat:.4f}_{lon:.4f}_{date}"
    
    def _celsius_to_fahrenheit(self, celsius: float) -> float:
        """Convert Celsius to Fahrenheit"""
        if celsius is None:
            return None
        return (celsius * 9/5) + 32
    
    def fetch_temperature_data(self, latitude: float, longitude: float, 
                              date: str) -> Optional[Dict]:
        """
        Fetch temperature data from Open-Meteo API for a specific location and date
        
        Args:
            latitude: Latitude of the location
            longitude: Longitude of the location
            date: Date in YYYY-MM-DD format
            
        Returns:
            Dictionary with temperature data or None if not available
        """
        # Check cache first
        cache_key = self._get_cache_key(latitude, longitude, date)
        if cache_key in self.cache:
            logger.debug(f"Cache hit for {cache_key}")
            return self.cache[cache_key]
        
        # Prepare API request parameters
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': date,
            'end_date': date,
            'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean',
            'temperature_unit': 'fahrenheit',
            'timezone': 'America/Chicago'  # Using central time for US cities
        }
        
        # Make API request with retries
        for attempt in range(MAX_RETRIES):
            try:
                logger.debug(f"Fetching data for lat={latitude}, lon={longitude}, date={date} (attempt {attempt+1})")
                
                response = self.session.get(OPENMETEO_URL, params=params, timeout=30)
                
                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Process the response
                    temp_data = self._process_response(data)
                    
                    # Cache the result
                    self.cache[cache_key] = temp_data
                    
                    if temp_data:
                        logger.info(f"✓ Retrieved temperature data for {date} at ({latitude:.4f}, {longitude:.4f}): "
                                  f"Max={temp_data['temp_max_f']:.1f}°F, Min={temp_data['temp_min_f']:.1f}°F, "
                                  f"Avg={temp_data['temp_avg_f']:.1f}°F")
                    else:
                        logger.warning(f"✗ No temperature data available for {date} at ({latitude}, {longitude})")
                    
                    return temp_data
                    
                elif response.status_code == 429:  # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded, waiting {RETRY_DELAY * (attempt + 1)}s...")
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                    
                else:
                    logger.error(f"API error {response.status_code}: {response.text}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY)
                        continue
                    else:
                        self.cache[cache_key] = None
                        return None
                        
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    self.cache[cache_key] = None
                    return None
        
        self.cache[cache_key] = None
        return None
    
    def _process_response(self, data: dict) -> Optional[Dict]:
        """
        Process API response and extract temperature data
        
        Args:
            data: JSON response from API
            
        Returns:
            Dictionary with temperature data or None
        """
        try:
            if 'daily' not in data:
                return None
            
            daily = data['daily']
            
            # Extract temperature values (should be single values since we query one date)
            temp_max = daily.get('temperature_2m_max', [None])[0]
            temp_min = daily.get('temperature_2m_min', [None])[0]
            temp_mean = daily.get('temperature_2m_mean', [None])[0]
            
            if temp_max is None and temp_min is None and temp_mean is None:
                return None
            
            result = {
                'temp_max_f': temp_max,
                'temp_min_f': temp_min,
                'temp_avg_f': temp_mean
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing response: {e}")
            return None

def process_csv(input_file: str, output_file: str, sample_size: Optional[int] = None):
    """
    Process the CSV file and add temperature data
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        sample_size: Optional number of rows to process (for testing)
    """
    logger.info(f"Reading CSV file: {input_file}")
    
    # Read CSV
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} rows")
    
    # Sample if requested
    if sample_size:
        df = df.head(sample_size)
        logger.info(f"Processing sample of {len(df)} rows")
    
    # Get unique city/date combinations
    unique_combos = df[['date', 'latitude', 'longitude', 'location']].drop_duplicates()
    logger.info(f"Found {len(unique_combos)} unique city/date combinations")
    
    # Initialize extractor
    extractor = OpenMeteoTemperatureExtractor()
    
    # Create a dictionary to store temperature data
    temp_data_dict = {}
    
    # Fetch temperature data for each unique combination
    total = len(unique_combos)
    start_time = time.time()
    
    for counter, (idx, row) in enumerate(unique_combos.iterrows()):
        progress = (counter + 1) / total * 100
        elapsed = time.time() - start_time
        eta = (elapsed / (counter + 1)) * (total - counter - 1) if counter > 0 else 0
        
        logger.info(f"Progress: {counter+1}/{total} ({progress:.1f}%) - ETA: {eta:.0f}s")
        
        temp_data = extractor.fetch_temperature_data(
            row['latitude'],
            row['longitude'],
            row['date']
        )
        
        # Store in dictionary with key as (date, lat, lon)
        key = (row['date'], row['latitude'], row['longitude'])
        temp_data_dict[key] = temp_data
        
        # Save cache periodically
        if (counter + 1) % 10 == 0:
            extractor._save_cache()
    
    # Save final cache
    extractor._save_cache()
    
    # Merge temperature data back to original dataframe
    logger.info("Merging temperature data with original CSV...")
    
    def get_temp_data(row):
        key = (row['date'], row['latitude'], row['longitude'])
        data = temp_data_dict.get(key, {})
        return pd.Series({
            'temp_max_f': data.get('temp_max_f') if data else None,
            'temp_min_f': data.get('temp_min_f') if data else None,
            'temp_avg_f': data.get('temp_avg_f') if data else None
        })
    
    temp_columns = df.apply(get_temp_data, axis=1)
    df = pd.concat([df, temp_columns], axis=1)
    
    # Save to output file
    logger.info(f"Saving results to: {output_file}")
    df.to_csv(output_file, index=False)
    
    # Print summary statistics
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    logger.info(f"Total rows processed: {len(df)}")
    logger.info(f"Unique city/date combinations: {len(unique_combos)}")
    logger.info(f"Records with temperature data: {df['temp_max_f'].notna().sum()}")
    logger.info(f"Records without temperature data: {df['temp_max_f'].isna().sum()}")
    logger.info(f"Success rate: {df['temp_max_f'].notna().sum()/len(df)*100:.1f}%")
    
    if df['temp_max_f'].notna().sum() > 0:
        logger.info(f"\nTemperature Statistics (Fahrenheit):")
        logger.info(f"  Max Temperature: {df['temp_max_f'].min():.1f}°F to {df['temp_max_f'].max():.1f}°F")
        logger.info(f"  Min Temperature: {df['temp_min_f'].min():.1f}°F to {df['temp_min_f'].max():.1f}°F")
        logger.info(f"  Avg Temperature: {df['temp_avg_f'].min():.1f}°F to {df['temp_avg_f'].max():.1f}°F")
    
    logger.info("="*60)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Extract temperature data from Open-Meteo for cities in CSV file'
    )
    parser.add_argument(
        '--input',
        default='Cleaned_US_City_Specefic_GeoCode.csv',
        help='Input CSV file path'
    )
    parser.add_argument(
        '--output',
        default='Cleaned_US_City_Specefic_GeoCode_with_Temperature.csv',
        help='Output CSV file path'
    )
    parser.add_argument(
        '--sample',
        type=int,
        help='Process only first N rows (for testing)'
    )
    
    args = parser.parse_args()
    
    try:
        process_csv(
            input_file=args.input,
            output_file=args.output,
            sample_size=args.sample
        )
        logger.info("✓ Processing complete!")
        
    except Exception as e:
        logger.error(f"✗ Error during processing: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())
