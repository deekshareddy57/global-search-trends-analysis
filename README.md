# Global Search Trends Data Pipeline & Analytics

This repository contains an end-to-end data pipeline for extracting, cleaning, merging, and analyzing global Google Search Trends data. It supports a faculty-led research project at Iowa State University and demonstrates large-scale data processing, automation, time-series analysis, and dashboard visualization using Python.


## 🔍 Project Overview

The pipeline extracts weekly Google Search Trends data across multiple countries and cities using Python and PyTrends. It includes automated request throttling, retry logic, error handling, region-based iteration, and standardized cleaning steps.

The workflow processes raw weekly time-series datasets into structured analytical outputs, enabling regional comparison, seasonal pattern detection, and dashboard visualization.

This project demonstrates skills in Python automation, large-scale data cleaning, data engineering logic, visualization, and analytical storytelling.


## 🛠 Tech Stack

- Python (pandas, numpy)
- PyTrends
- Asyncio (throttling, retries)
- Matplotlib / Seaborn
- Jupyter Notebooks
- Git/GitHub


## ⚙️ Features

### ✔ Automated Data Extraction
- Weekly Google Search Trends extraction  
- Handles multiple countries, regions, and cities  
- Request throttling to avoid rate limits  
- Retry and error-handling logic  

### ✔ Data Cleaning & Standardization
- Timestamp alignment  
- Normalizing region/city identifiers  
- Handling missing or incomplete weekly values  
- Merging multi-year, multi-country datasets  

### ✔ Analysis & Visualization
- Time-series trend charts  
- Regional comparison plots  
- Keyword-level behavior analysis  
- Outlier detection (optional)  

### ✔ Interactive Dashboard
Built in Power BI with:
- Time-series slicers  
- Region/country filters  
- Keyword-level drilldown  
- Exportable insight views  

---

## 📊 Sample Data
The repository includes **sample raw and cleaned data (50–200 rows)** to demonstrate the pipeline.

Full datasets are **not included** due to research agreements and ethical data-sharing constraints.

---

## 📈 Key Insights (Example Findings)
- Seasonal search interest peaks consistently in **Q2–Q3** across most regions.  
- East Asian cities show **higher volatility** in weekly search interest.  
- U.S. midwestern cities display **stable patterns** with minimal variation.  
- Fishing-related terms differ significantly by climate, geography, and culture.  

---

## 🚀 How to Run

### 1. Clone the repository
git clone https://github.com/deekshareddy57/global-search-trends-analysis.git
cd global-search-trends-analysis

### 2. Install dependencies
pip install -r requirements.txt

### 3. Run data extraction
python scripts/extract_data.py

### 4. Run data cleaning
python scripts/clean_data.py

### 5. Open notebooks for analysis
notebooks/01_extraction.ipynb
notebooks/02_cleaning.ipynb
notebooks/03_visualization.ipynb
notebooks/04_analysis.ipynb

## 📊 Dashboard Preview

[yet to insert the picture]

## 📧 Contact

Deeksha Reddy 
deekshareddy.c@gmail.com
https://www.linkedin.com/in/deeksha-reddy/
