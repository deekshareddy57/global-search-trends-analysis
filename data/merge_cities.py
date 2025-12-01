import pandas as pd
import os
import glob
import logging

# === Configuration ===
# Define the root directory where all your country folders (trends_...) are located.
ROOT_DIR = "raw_samples" 
# Define the directory where the merged country files will be saved.
OUTPUT_DIR = "merged_country_files"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Helper Function ---
def merge_country_data(country_folder_name: str):
    """
    Reads all CSV files within the subdirectories of a given country folder
    and merges them into a single country-level CSV file.
    """
    country_path = os.path.join(ROOT_DIR, country_folder_name)
    
    if not os.path.isdir(country_path):
        logger.error(f"Directory not found: {country_path}")
        return

    # Use glob to find all CSV files inside any subdirectory (*/*/*.csv)
    # This pattern accounts for the structure: 'trends_country/STATE_CODE/file.csv'
    all_files = glob.glob(os.path.join(country_path, "**", "*.csv"), recursive=True)
    
    if not all_files:
        logger.warning(f"No CSV files found in subdirectories of {country_folder_name}. Skipping.")
        return

    logger.info(f"Found {len(all_files)} files for {country_folder_name}. Starting merge...")
    
    # List to hold DataFrames
    list_df = []
    
    # Read each file and append to the list
    for filename in all_files:
        try:
            df = pd.read_csv(filename)
            list_df.append(df)
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")

    if not list_df:
        logger.warning(f"Failed to read any dataframes for {country_folder_name}.")
        return

    # Concatenate all DataFrames in the list
    merged_df = pd.concat(list_df, ignore_index=True)

    # Clean up the country name for the output file
    # Example: 'trends_australia_2024_empty' -> 'AUSTRALIA'
    country_name = country_folder_name.split('_')[1].upper()
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Define the output filename
    output_filename = f"{country_name}_2024_MERGED.csv"
    output_filepath = os.path.join(OUTPUT_DIR, output_filename)
    
    # Save the merged file
    merged_df.to_csv(output_filepath, index=False)
    logger.info(f"✅ Successfully merged and saved: {output_filepath}")

# --- Main Execution ---

def main_merge_all_countries():
    """
    Identifies all country data folders and calls the merge function for each.
    """
    # Get all immediate subdirectories within the ROOT_DIR
    all_subfolders = [d for d in os.listdir(ROOT_DIR) if os.path.isdir(os.path.join(ROOT_DIR, d))]
    
    # Filter for folders that start with 'trends_'
    country_folders = [f for f in all_subfolders if f.startswith('trends_')]

    if not country_folders:
        logger.warning(f"No 'trends_' folders found in the root directory: {ROOT_DIR}")
        return

    logger.info(f"Found {len(country_folders)} country folders to process.")
    
    # 
    for folder in country_folders:
        merge_country_data(folder)

if __name__ == "__main__":
    main_merge_all_countries()