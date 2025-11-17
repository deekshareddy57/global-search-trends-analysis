import asyncio
import pandas as pd
import sys
import os

# Get the parent directory (FishingTrends_New)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

sys.path.insert(0, parent_dir)

# Build correct file path
CSV_PATH = os.path.join(parent_dir, "data", "locations", "usa_cities.csv")


# Now import from extraction folder
try:
    from extraction.usa_extraction import TrendsExtractor, FISHING_SEARCH_TERMS
    print("[OK] Successfully imported TrendsExtractor")
except ImportError as e:
    print(f"✗ Import Error: {e}")
    print(f"Current directory: {current_dir}")
    print(f"Parent directory: {parent_dir}")
    print(f"Looking for: {os.path.join(parent_dir, 'extraction', 'trends_extractor_us.py')}")
    print(f"File exists: {os.path.exists(os.path.join(parent_dir, 'extraction', 'usa_extraction.py'))}")
    raise

# === CONFIGURATION === 
#load data
try:
    print(">>> DEBUG CHECK <<<")
    print("CSV_PATH =", CSV_PATH, type(CSV_PATH))
    print("open =", open, type(open))
    print("os.path.join =", os.path.join, type(os.path.join))
    print("pd.read_csv =", pd.read_csv, type(pd.read_csv))

    #INPUT_CSV = pd.read_csv(CSV_PATH)
    print(f"[OK] Loaded {CSV_PATH}")
except FileNotFoundError:
    print("✗ CSV NOT FOUND:", CSV_PATH)
    print("Check that file exists at:", os.path.join(parent_dir, "data", "locations"))
    raise
OUTPUT_DIR = "data/raw_samples/usa_trends"
YEARS = [2024]  # Set to 2024 as per your requirement

def load_cities(file_path):
    """Load cities from CSV file"""
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} cities from {file_path}")
    
    # Verify geo_code column exists
    if 'geo_code' not in df.columns:
        print("WARNING: 'geo_code' column not found in CSV!")
        print("Available columns:", df.columns.tolist())
        raise ValueError("CSV must contain 'geo_code' column")
    
    # Show sample of cities with their geo codes
    print("\nSample cities:")
    print(df[['location_name', 'state_province', 'geo_code']].head())
    
    return df.to_dict('records')

async def extract_all_years_for_city(city_info, extractor):
    """Extract data for all specified years for a single city"""
    for year in YEARS:
        print(f"\n{'='*60}")
        print(f"Processing: {city_info['location_name']}, {city_info['state_province']}")
        print(f"Geo Code: {city_info.get('geo_code', 'N/A')}")
        print(f"Year: {year}")
        print(f"{'='*60}")
        
        await extractor.extract_city_year(city_info, year, FISHING_SEARCH_TERMS, OUTPUT_DIR)
        
        # Sleep between years to avoid rate limiting
        await asyncio.sleep(5)

async def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("US CITIES GOOGLE TRENDS EXTRACTOR")
    print("="*60)
    
    # Load cities
    cities = load_cities(CSV_PATH)
    print(f"\nTotal cities to process: {len(cities)}")
    print(f"Years: {YEARS}")
    print(f"Search terms: {len(FISHING_SEARCH_TERMS)}")
    print(f"Output directory: {OUTPUT_DIR}\n")
    
    # Initialize extractor
    extractor = TrendsExtractor()
    
    # Process each city
    for idx, city in enumerate(cities, 1):
        print(f"\n[{idx}/{len(cities)}] Starting: {city['location_name']}")
        
        try:
            await extract_all_years_for_city(city, extractor)
            print(f"✓ Completed: {city['location_name']}")
        except Exception as e:
            print(f"✗ Error processing {city['location_name']}: {e}")
            # Log error but continue with next city
            extractor.logger.error(f"Failed to process {city['location_name']}: {e}")
        
        # Sleep between cities to avoid rate limiting
        print(f"Waiting 10 seconds before next city...")
        await asyncio.sleep(10)
    
    print("\n" + "="*60)
    print("EXTRACTION COMPLETE!")
    print("="*60)
    print(f"Check '{OUTPUT_DIR}' for output files")
    print(f"Check 'logs/us_trends.log' for detailed logs")

if __name__ == "__main__":
    asyncio.run(main())