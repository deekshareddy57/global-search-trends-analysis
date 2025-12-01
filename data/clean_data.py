import pandas as pd
import glob
import os
import re # For regular expression matching

# === Configuration ===
MERGED_DIR = "data/merged_country_files"
HARMONIZED_DIR = "data/harmonized_long_format"
os.makedirs(HARMONIZED_DIR, exist_ok=True)

# Columns that uniquely identify a row before melting
ID_VARS = [
    'date', 'latitude', 'longitude', 'country', 'state', 
    'location', 'geo_code', 'year', 'language_code', 
    # Use 'state_code' if present in the data (like in Australia/Germany)
]

# --- Helper Function ---
def get_search_term_columns(df):
    """
    Identifies all columns that contain search term counts based on known patterns.
    """
    # 1. Standard (English-only) terms
    # Look for any column that is a single, capital-cased fishing search term
    # E.g., 'Fishing', 'Bass Fishing'
    standard_terms = [col for col in df.columns if col in ['Fishing', 'Fishing License', 'Fishing Conditions', 'Where to fish', 'Cleaning Fish', 'Cooking Fish', 'Fishing Rod', 'Fishing Boat', 'Fishing nets', 'Fishing Bait', 'Bass Fishing', 'Trout Fishing', 'Fly Fishing']]
    
    # 2. Bilingual terms (Germany/Japan/etc.)
    # Look for columns following the '_count' pattern (DE) or the term itself (JP/other)
    # This also finds the bilingual pairs defined in the German script
    bilingual_patterns = {
        'german': re.compile(r'(english_term|german_term)'),
        'japanese': re.compile(r'[a-zA-Z]+$') # Columns ending in a recognized language name, or the search term itself
    }
    
    # We will rely on a robust melt strategy below instead of pre-filtering all
    return [col for col in df.columns if col not in ID_VARS and not col.startswith('Unnamed:')]
def melt_and_harmonize_data(filepath):
    """
    Loads a single country file and transforms it from wide to long (melted) format,
    while standardizing the column names for bilingual data.
    """
    df = pd.read_csv(filepath)
    country_name = os.path.basename(filepath).split('_')[0]
    
    # --- 1. Identify ID Variables ---
    # Filter ID_VARS to only include columns actually present in the DataFrame
    id_vars_present = [col for col in ID_VARS if col in df.columns]
    
    # --- 2. Rename and Prepare Bilingual Columns (Crucial for Germany) ---
    if 'language_code' in df.columns and df['language_code'].iloc[0] == 'bilingual':
        # This is the German/Bilingual data structure
        
        # Melt the *specific* English and German count columns
        df_melted = pd.melt(
            df, 
            id_vars=id_vars_present + ['english_term', 'german_term', 'search_term_display'],
            value_vars=[col for col in df.columns if col.endswith('_count')],
            var_name='source_term_type',
            value_name='search_count'
        )
        
        # Use the 'source_term_type' to fill the final term columns
        df_melted['english_search_term'] = df_melted.apply(
            lambda row: row['english_term'] if 'english' in row['source_term_type'] else None, axis=1
        )
        df_melted['translated_search_term'] = df_melted.apply(
            lambda row: row['german_term'] if 'german' in row['source_term_type'] else None, axis=1
        )
        # Harmonize the terms for the final output
        df_melted['harmonized_search_term'] = df_melted['search_term_display']
        
        # Clean up columns
        df_melted = df_melted.drop(columns=['english_term', 'german_term', 'search_term_display', 'source_term_type'])

    # --- 3. Standard Melting (For Australia, US, or simple countries) ---
    else:
        # For standard, non-bilingual files (or files with terms as column headers)
        
        # Get all columns that contain term scores
        value_vars = get_search_term_columns(df)

        df_melted = pd.melt(
            df, 
            id_vars=id_vars_present,
            value_vars=value_vars,
            var_name='harmonized_search_term',
            value_name='search_count'
        )
        
        # Add the bilingual columns but fill them only with the term name
        df_melted['english_search_term'] = df_melted['harmonized_search_term']
        df_melted['translated_search_term'] = None
    
    # --- 4. Final Cleanup and Save ---
    
    # Ensure all final columns are present for merging
    FINAL_COLUMNS = [
        'date', 'country', 'state', 'location', 'geo_code', 
        'latitude', 'longitude', 'year', 'language_code',
        'harmonized_search_term', 'english_search_term', 
        'translated_search_term', 'search_count'
    ]
    
    # Add missing final columns with NaN
    for col in FINAL_COLUMNS:
        if col not in df_melted.columns:
            df_melted[col] = None
    
    df_melted = df_melted[FINAL_COLUMNS]
    
    # Save the harmonized file
    output_filename = f"{country_name}_2024_HARMONIZED.csv"
    output_filepath = os.path.join(HARMONIZED_DIR, output_filename)
    df_melted.to_csv(output_filepath, index=False)
    print(f"✅ Harmonized and saved: {output_filename}")
    
    return df_melted

# --- Main Execution ---

def main_harmonize_all():
    all_merged_files = glob.glob(os.path.join(MERGED_DIR, "*_MERGED.csv"))
    
    if not all_merged_files:
        print(f"Error: No merged files found in {MERGED_DIR}. Please run the merge script first.")
        return

    print(f"Starting harmonization of {len(all_merged_files)} country files...")
    
    all_harmonized_data = []
    
    for filepath in all_merged_files:
        all_harmonized_data.append(melt_and_harmonize_data(filepath))
        
    # Concatenate all harmonized country files into one global dataset
    global_harmonized_df = pd.concat(all_harmonized_data, ignore_index=True)
    
    global_output_filepath = os.path.join(HARMONIZED_DIR, "GLOBAL_ALL_HARMONIZED.csv")
    global_harmonized_df.to_csv(global_output_filepath, index=False)
    print("\n-------------------------------------------")
    print(f"🎉 FINAL STEP COMPLETE! Global file created: {global_output_filepath}")
    print("-------------------------------------------")

if __name__ == "__main":
    main_harmonize_all()