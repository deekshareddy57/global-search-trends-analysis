import pandas as pd
import numpy as np
from datetime import datetime

def analyze_phenology(input_file, output_file='phenology_analysis.csv'):
    """
    Analyze search phenology patterns from raw weekly data.
    Extracts key seasonal metrics: start, peak, end, duration, and statistics.
    
    Parameters:
    input_file (str): Path to input CSV file
    output_file (str): Path to output CSV file
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Ensure date is datetime and search_count is numeric
    df['date'] = pd.to_datetime(df['date'])
    df['search_count'] = pd.to_numeric(df['search_count'], errors='coerce').fillna(0).astype(int)
    
    # Group by location, search term, and year
    results = []
    
    for (location, search_term, year), group in df.groupby(['location', 'search_term', 'year']):
        group = group.sort_values('date').reset_index(drop=True)
        
        # Skip if no data
        if len(group) == 0:
            continue
        
        # Get location metadata
        geo_code = group['geo_code'].iloc[0]
        state = group['state'].iloc[0]
        country = group['country'].iloc[0]
        
        # Extract values
        dates = group['date'].values
        search_counts = group['search_count'].values
        
        # Total searches
        total_searches = int(np.sum(search_counts))
        
        # Find non-zero entries
        non_zero_mask = search_counts > 0
        non_zero_indices = np.where(non_zero_mask)[0]
        
        # Season Start: first non-zero
        if len(non_zero_indices) > 0:
            start_idx = non_zero_indices[0]
            season_start_date = pd.to_datetime(dates[start_idx]).strftime('%Y-%m-%d')
            season_start_count = int(search_counts[start_idx])
        else:
            season_start_date = None
            season_start_count = 0
            start_idx = None
        
        # Season End: last non-zero
        if len(non_zero_indices) > 0:
            end_idx = non_zero_indices[-1]
            season_end_date = pd.to_datetime(dates[end_idx]).strftime('%Y-%m-%d')
            season_end_count = int(search_counts[end_idx])
        else:
            season_end_date = None
            season_end_count = 0
            end_idx = None
        
        # Peak: maximum value
        peak_idx = np.argmax(search_counts)
        peak_date = pd.to_datetime(dates[peak_idx]).strftime('%Y-%m-%d')
        peak_count = int(search_counts[peak_idx])
        
        # Duration in weeks and days
        if start_idx is not None and end_idx is not None:
            duration_weeks = end_idx - start_idx
            duration_days = (pd.to_datetime(dates[end_idx]) - pd.to_datetime(dates[start_idx])).days
        else:
            duration_weeks = 0
            duration_days = 0
        
        # Statistics for active season (non-zero weeks)
        non_zero_counts = search_counts[non_zero_mask]
        if len(non_zero_counts) > 0:
            avg_count_active = float(np.mean(non_zero_counts))
            min_count_active = int(np.min(non_zero_counts))
            max_count_active = int(np.max(non_zero_counts))
            num_active_weeks = len(non_zero_counts)
        else:
            avg_count_active = 0.0
            min_count_active = 0
            max_count_active = 0
            num_active_weeks = 0
        
        # Median - calculate both for all weeks and active weeks only
        median_all_weeks = float(np.median(search_counts))
        
        if len(non_zero_counts) > 0:
            median_active_weeks = float(np.median(non_zero_counts))
        else:
            median_active_weeks = 0.0
        
        # Median crossings (using all weeks median)
        # Median crossings (using all weeks median)
        median_crossings = 0
        for i in range(1, len(search_counts)):
            if (search_counts[i-1] < median_all_weeks and search_counts[i] >= median_all_weeks) or \
               (search_counts[i-1] >= median_all_weeks and search_counts[i] < median_all_weeks):
                median_crossings += 1
        
        # Statistics for active season (non-zero weeks)
        non_zero_counts = search_counts[non_zero_mask]
        if len(non_zero_counts) > 0:
            avg_count_active = float(np.mean(non_zero_counts))
            min_count_active = int(np.min(non_zero_counts))
            max_count_active = int(np.max(non_zero_counts))
            num_active_weeks = len(non_zero_counts)
        else:
            avg_count_active = 0.0
            min_count_active = 0
            max_count_active = 0
            num_active_weeks = 0
        
        # Total weeks in dataset
        total_weeks = len(search_counts)
        
        # Compile results
        result = {
            'location': location,
            'latitude': group['latitude'].iloc[0],
            'longitude': group['longitude'].iloc[0],
            'geo_code': geo_code,
            'state': state,
            'country': country,
            'search_term': search_term,
            'year': int(year),
            'season_start_date': season_start_date if season_start_date else 'N/A',
            'season_start_count': season_start_count,
            'peak_date': peak_date,
            'peak_count': peak_count,
            'season_end_date': season_end_date if season_end_date else 'N/A',
            'season_end_count': season_end_count,
            'duration_weeks': duration_weeks,
            'duration_days': duration_days,
            'num_active_weeks': num_active_weeks,
            'total_weeks': total_weeks,
            'median_all_weeks': round(median_all_weeks, 1),
            'median_active_weeks': round(median_active_weeks, 1),
            'median_crossings': median_crossings,
            'avg_count_active_weeks': round(avg_count_active, 1),
            'min_count_active_weeks': min_count_active,
            'max_count_active_weeks': max_count_active,
            'total_searches': total_searches
        }
        
        results.append(result)
    
    # Create output dataframe
    results_df = pd.DataFrame(results)
    
    # Save to CSV
    results_df.to_csv(output_file, index=False)
    
    print(f"✅ Analysis complete! Results saved to {output_file}")
    print(f"📊 Total records analyzed: {len(results_df)}")
    print(f"📍 Unique locations: {results_df['location'].nunique()}")
    print(f"🔍 Unique search terms: {results_df['search_term'].nunique()}")
    print(f"\n📋 Sample of results:")
    print(results_df[['location', 'search_term', 'year', 'season_start_date', 'peak_date', 'peak_count', 'total_searches']].head(10))
    
    return results_df


if __name__ == "__main__":
    # Example usage
    input_file = "usa_cities.csv"  # Change this to your input file path
    output_file = "phenology_analysis_with_lat_lon.csv"  # Change this to your desired output path
    
    # Run analysis
    results = analyze_phenology(input_file, output_file)