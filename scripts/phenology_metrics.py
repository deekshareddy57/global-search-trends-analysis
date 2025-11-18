import pandas as pd
import numpy as np
from scipy.ndimage import gaussian_filter1d
from scipy.interpolate import interp1d
from datetime import datetime

def analyze_phenology(input_file, output_file='phenology_analysis.csv', 
                     sigma=4, threshold_pct=20):
    """
    Analyze search phenology patterns using Gaussian smoothing and threshold detection.
    
    Parameters:
    input_file (str): Path to input CSV file
    output_file (str): Path to output CSV file
    sigma (float): Smoothing factor for Gaussian filter (default: 4)
    threshold_pct (int): Threshold percentage above minimum (default: 20)
    """
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Ensure date is datetime and search_count is numeric
    df['date'] = pd.to_datetime(df['date'])
    df['search_count'] = pd.to_numeric(df['search_count'], errors='coerce').fillna(0)
    df['day_of_year'] = df['date'].dt.dayofyear
    
    # Group by location, search term, and year
    results = []
    
    for (location, search_term, year), group in df.groupby(['location', 'search_term', 'year']):
        group = group.sort_values('day_of_year').reset_index(drop=True)
        
        # Skip if no data
        if len(group) == 0:
            continue
        
        # Get location metadata
        geo_code = group['geo_code'].iloc[0]
        state = group['state'].iloc[0]
        country = group['country'].iloc[0]
        
        # Extract weekly values
        doy = group['day_of_year'].values
        search_counts = group['search_count'].values
        
        # Create continuous interpolated data
        doy_continuous = np.arange(doy.min(), doy.max() + 1)
        interpolator = interp1d(doy, search_counts, kind='linear', fill_value='extrapolate')
        search_counts_continuous = interpolator(doy_continuous)
        
        # Smooth the search trend using Gaussian filter
        smoothed_counts = gaussian_filter1d(search_counts_continuous, sigma=sigma)
        
        # Define threshold
        min_val = smoothed_counts.min()
        max_val = smoothed_counts.max()
        threshold = min_val + (threshold_pct / 100) * (max_val - min_val)
        
        # Find Peak (B)
        peak_index = np.argmax(smoothed_counts)
        B_doy = int(doy_continuous[peak_index])
        B_value = smoothed_counts[peak_index]
        
        # Find Start (A) - first point above threshold before peak
        indices_before_peak = np.where(doy_continuous < B_doy)[0]
        above_thresh_before_peak = smoothed_counts[indices_before_peak] > threshold
        
        if np.any(above_thresh_before_peak):
            start_index = indices_before_peak[np.argmax(above_thresh_before_peak)]
            A_doy = int(doy_continuous[start_index])
            A_value = smoothed_counts[start_index]
        else:
            A_doy = int(doy_continuous[0])
            A_value = smoothed_counts[0]
            start_index = 0
        
        # Find End (C) - first point below threshold after peak
        indices_after_peak = np.where(doy_continuous > B_doy)[0]
        
        if len(indices_after_peak) > 0:
            below_thresh_after_peak = smoothed_counts[indices_after_peak] <= threshold
            if np.any(below_thresh_after_peak):
                end_index = indices_after_peak[np.argmax(below_thresh_after_peak)]
            else:
                end_index = len(smoothed_counts) - 1
        else:
            end_index = len(smoothed_counts) - 1
        
        C_doy = int(doy_continuous[end_index])
        C_value = smoothed_counts[end_index]
        
        # Calculate duration
        duration_days = C_doy - A_doy
        
        # Calculate additional metrics
        non_zero_counts = search_counts[search_counts > 0]
        
        # Median and median crossings
        median_count = np.median(search_counts)
        median_crossings = 0
        for i in range(1, len(search_counts)):
            if (search_counts[i-1] < median_count and search_counts[i] >= median_count) or \
               (search_counts[i-1] >= median_count and search_counts[i] < median_count):
                median_crossings += 1
        
        # Average and total in season (between A and C)
        season_mask = (doy >= A_doy) & (doy <= C_doy)
        season_counts = search_counts[season_mask]
        season_counts_nonzero = season_counts[season_counts > 0]
        
        if len(season_counts_nonzero) > 0:
            avg_count_in_season = np.mean(season_counts_nonzero)
        else:
            avg_count_in_season = 0
        
        total_searches = int(np.sum(search_counts))
        total_season_searches = int(np.sum(season_counts))
        
        # Find actual dates for A, B, C
        date_A = group[group['day_of_year'] <= A_doy]['date'].iloc[-1] if np.any(group['day_of_year'] <= A_doy) else group['date'].iloc[0]
        date_B = group[group['day_of_year'] <= B_doy]['date'].iloc[-1] if np.any(group['day_of_year'] <= B_doy) else group['date'].iloc[0]
        date_C = group[group['day_of_year'] <= C_doy]['date'].iloc[-1] if np.any(group['day_of_year'] <= C_doy) else group['date'].iloc[-1]
        
        # Compile results
        result = {
            'location': location,
            'geo_code': geo_code,
            'state': state,
            'country': country,
            'search_term': search_term,
            'year': year,
            'season_start_doy': A_doy,
            'season_start_date': date_A.strftime('%Y-%m-%d'),
            'peak_doy': B_doy,
            'peak_date': date_B.strftime('%Y-%m-%d'),
            'season_end_doy': C_doy,
            'season_end_date': date_C.strftime('%Y-%m-%d'),
            'duration_days': duration_days,
            'duration_weeks': round(duration_days / 7, 1),
            'start_value_smoothed': round(A_value, 2),
            'peak_value_smoothed': round(B_value, 2),
            'end_value_smoothed': round(C_value, 2),
            'threshold_value': round(threshold, 2),
            'median_count': round(median_count, 1),
            'median_crossings': median_crossings,
            'avg_count_in_season': round(avg_count_in_season, 1),
            'total_searches_year': total_searches,
            'total_searches_season': total_season_searches,
            'max_raw_count': int(search_counts.max()),
            'min_raw_count': int(search_counts.min())
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
    print(f"\n📋 First few rows:")
    print(results_df.head())
    print(f"\n📈 Summary statistics:")
    print(results_df[['duration_days', 'peak_value_smoothed', 'total_searches_year']].describe())
    
    return results_df


if __name__ == "__main__":
    # Example usage
    input_file = "usa_cities.csv"  # Change this to your input file path
    output_file = "phenology_metrics.csv"  # Change this to your desired output path
    
    # Run analysis with default parameters (sigma=4, threshold_pct=20)
    # You can adjust these to match your Streamlit app settings
    results = analyze_phenology(
        input_file, 
        output_file,
        sigma=4,  # Smoothing factor (1-10)
        threshold_pct=20  # Threshold percentage (10-50)
    )
    
    # Optional: Try different parameters
    # results = analyze_phenology(input_file, "phenology_sigma6.csv", sigma=6, threshold_pct=25)