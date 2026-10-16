import pandas as pd
import requests

API_URL = "http://localhost:8000/data/batch"
CSV_FILE_PATH = "data.csv"

def run_pandas_pipeline():
    # ----------------------------------------------------
    # 1. EXTRACT: Read the CSV file into a pandas DataFrame
    # ----------------------------------------------------
    print(f"[ETL] Extracting data using pandas from: {CSV_FILE_PATH}...")
    try:
        # na_values tells pandas to automatically treat 'NULL' text strings as real missing data (NaN)
        df = pd.read_csv(CSV_FILE_PATH, na_values=["NULL"])
    except FileNotFoundError:
        print(f"[ETL] Error: {CSV_FILE_PATH} not found. Run generate_csv.py first!")
        return

    initial_count = len(df)

    # ----------------------------------------------------
    # 2. TRANSFORM: Clean up columns in bulk
    # ----------------------------------------------------
    print("[ETL] Transforming dataset with pandas vectorisation...")
    
    # Drop rows where 'raw_val' or 'id' is missing (NaN)
    df = df.dropna(subset=["raw_val", "id"])
    dropped_count = initial_count - len(df)
    if dropped_count > 0:
        print(f" [Skip] Dropped {dropped_count} rows with missing or NULL metrics.")

    # Clean the string data: Strip outer whitespace and replace internal underscores with spaces
    df["raw_name"] = df["raw_name"].astype(str).str.strip().str.replace("_", " ")

    # Cast numeric columns to their proper native Python datatypes
    df["id"] = df["id"].astype(int)
    df["raw_val"] = df["raw_val"].astype(float)

    # Rename columns to match what our FastAPI Pydantic schema expects
    df = df.rename(columns={"raw_name": "name", "raw_val": "value"})

    # ----------------------------------------------------
    # 3. LOAD: Convert DataFrame to a dictionary and POST it
    # ----------------------------------------------------
    # 'records' format outputs: [{"id": 1001, "name": "widget alpha", "value": 45.99}, ...]
    cleaned_records = df.to_dict(orient="records")
    
    # 🚨 ADD THIS LINE to see exactly what data is being sent
    print(f"[DEBUG] Raw payload data sample: {cleaned_records[:2]}")
    
    print(f"[ETL] Loading {len(cleaned_records)} sanitised elements into FastAPI...")
    try:
        response = requests.post(API_URL, json=cleaned_records)
        if response.status_code == 200:
            print("[ETL] Load step complete!", response.json())
        else:
            print(f"[ETL] FastAPI rejected payload. Status: {response.status_code}")
            # 🚨 ADD THIS LINE to see the exact validation error reason from FastAPI
            print(f"[DEBUG] FastAPI Error Reason: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("[ETL] Network Failure: Ensure your FastAPI terminal server is alive.")

if __name__ == "__main__":
    run_pandas_pipeline()
