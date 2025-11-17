import os
import logging
from datetime import datetime
import pandas as pd
from pytrends.request import TrendReq
import asyncio
from asyncio_throttle import Throttler
from typing import List, Dict, Optional

# === GLOBAL SEARCH TERMS ===
FISHING_SEARCH_TERMS = [
    "Fishing", "Fishing License", "Fishing Conditions", "Where to fish", "Cleaning Fish",
    "Cooking Fish", "Fishing Rod", "Fishing Boat", "Fishing nets", "Fishing Bait",
    "Bass Fishing", "Trout Fishing", "Fly Fishing"
]

# === Settings ===
REQUEST_DELAY = 120
RETRY_ATTEMPTS = 3
RETRY_DELAY = 60


class TrendsExtractor:
    def __init__(self):
        self.pytrends = TrendReq(hl='en-US', tz=360)
        self.throttler = Throttler(rate_limit=1, period=60)
        self.logger = self.setup_logger()

    def setup_logger(self):
        logger = logging.getLogger("USTrendsLogger")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File handler
        os.makedirs("logs", exist_ok=True)
        fh = logging.FileHandler("logs/us_trends.log")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger

    def get_timeframe(self, year: int):
        start = f"{year}-01-01"
        if year == datetime.now().year:
            end = datetime.now().strftime("%Y-%m-%d")
        else:
            end = f"{year}-12-31"
        return f"{start} {end}"

    async def fetch_data(self, terms: List[str], location: str, timeframe: str, geo_code: str) -> Optional[pd.DataFrame]:
        """
        Fetch Google Trends data for given terms and location.
        
        KEY CHANGE: Now uses the city-specific geo_code instead of country_code
        """
        async with self.throttler:
            try:
                all_data = None
                for i in range(0, len(terms), 5):
                    batch = terms[i:i + 5]
                    # IMPORTANT: Use the geo_code parameter which should be the metro-level code
                    self.logger.info(f"Fetching trends for {location} with geo code: {geo_code}")
                    self.pytrends.build_payload(batch, cat=0, timeframe=timeframe, geo=geo_code)
                    df = self.pytrends.interest_over_time()
                    
                    if 'isPartial' in df.columns:
                        df = df.drop('isPartial', axis=1)
                    
                    if df.empty:
                        self.logger.warning(f"Empty dataframe for {location} - batch {i//5 + 1}")
                        continue
                    
                    all_data = df if all_data is None else pd.concat([all_data, df], axis=1)
                    await asyncio.sleep(REQUEST_DELAY)
                
                return all_data
            except Exception as e:
                self.logger.error(f"Error fetching trends for {location} [{geo_code}] [{timeframe}]: {e}")
                return None

    async def extract_city_year(self, city: Dict, year: int, search_terms: List[str], output_dir: str) -> Dict[str, bool]:
        location = city['location_name']
        geo_code = city.get('geo_code', f"US-{city['state_province']}")
        
        lat, lon = city['latitude'], city['longitude']
        country = city['country']
        state = city['state_province']
        lang = city.get('language_code', 'en')
        timeframe = self.get_timeframe(year)

        self.logger.info(f"Extracting: {location}, {state} (geo: {geo_code}) - {year}")
        df = await self.fetch_data(search_terms, location, timeframe, geo_code)
        results = {}

        # FIX: Ensure df is valid before accessing columns
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            self.logger.warning(f"No data returned for {location} - {year}")
            return {f"{location}_{t}_{year}.csv": False for t in search_terms}

        # FIX: Safe column checking
        for term in search_terms:
            if term not in df.columns:
                results[f"{location}_{term}_{year}.csv"] = False
                self.logger.warning(f"✗ Term '{term}' not found in results for {location}")
                continue

            term_df = pd.DataFrame({
                'date': df.index,
                'latitude': lat,
                'longitude': lon,
                'country': country,
                'state': state,
                term: df[term].values,
                'location': location,
                'geo_code': geo_code,
                'search_term': term,
                'year': year,
                'language_code': lang
            })

            clean_city = location.replace(' ', '_').replace(',', '').replace("'", '').replace('-', '_')
            clean_term = term.replace(' ', '_').replace(',', '').replace("'", '').replace('-', '_')
            filename = f"{clean_city}_{clean_term}_{year}.csv"
            filepath = os.path.join(output_dir, filename)
            os.makedirs(output_dir, exist_ok=True)

            term_df.to_csv(filepath, index=False)
            results[filename] = True
            self.logger.info(f"[OK] Saved: {filename} (geo: {geo_code})")

        return results



__all__ = ["TrendsExtractor", "FISHING_SEARCH_TERMS"]