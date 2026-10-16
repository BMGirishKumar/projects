import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

# --- DATABASE ENGINE SETUP ---
# This creates 'supply_chain_dw.db' in the exact folder where this script runs
DB_FILE = "supply_chain_dw.db"
DB_URL = f"sqlite:///{DB_FILE}"
engine = create_engine(DB_URL)

def run_etl_pipeline(file_path):
    print("🚀 Initializing WMS Inventory ETL Pipeline (SQLite Engine)...")
    
    # ----------------------------------------------------
    # 1. EXTRACT STEP
    # ----------------------------------------------------
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ Error: Source file missing at: {file_path}")
    
    df_raw = pd.read_csv(file_path)
    print(f"📥 Extracted {len(df_raw)} records from raw WMS file.")

    # ----------------------------------------------------
    # 2. TRANSFORM STEP (Data Cleaning & Quality Guardrails)
    # ----------------------------------------------------
    print("🧹 Executing structural transformations...")
    
    # Uniformity: Remove spaces and force uppercase on SKUs
    df_raw['sku'] = df_raw['sku'].str.strip().str.upper()
    
    # Missing Value Handling: Fix null categories and names
    df_raw['category'] = df_raw['category'].fillna('UNKNOWN').str.strip().str.title()
    df_raw['product_name'] = df_raw['product_name'].fillna('MISSING NAME').str.strip()
    
    # Dates: Format into standard text dates
    df_raw['snapshot_date'] = pd.to_datetime(df_raw['snapshot_date']).dt.date.astype(str)
    
    # Type Casting: Force correct numerical formats
    df_raw['quantity_on_hand'] = pd.to_numeric(df_raw['quantity_on_hand'], errors='coerce').fillna(0).astype(int)
    df_raw['unit_cost_usd'] = pd.to_numeric(df_raw['unit_cost_usd'], errors='coerce').fillna(0.0)
    
    # Guardrail: Drop negative quantities completely
    df_raw = df_raw[df_raw['quantity_on_hand'] >= 0]
    
    # Derived Logic: Calculate overall monetary value
    df_raw['total_inventory_value'] = df_raw['quantity_on_hand'] * df_raw['unit_cost_usd']

    # ----------------------------------------------------
    # 3. LOAD STEP (Populating the Star Schema)
    # ----------------------------------------------------
    with engine.begin() as conn:
        print("Initializing relational database schemas...")
        
        # Build relational analytical layer matching Star Schema constraints
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_products (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT UNIQUE,
                product_name TEXT,
                category TEXT
            );
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fact_inventory (
                fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_actual TEXT,
                product_key INTEGER,
                quantity_on_hand INTEGER,
                unit_cost_usd REAL,
                total_inventory_value REAL,
                FOREIGN KEY(product_key) REFERENCES dim_products(product_key)
            );
        """))

        print("💾 Loading dimensions...")
        # Isolate distinct products based on cleaned unique SKUs
        distinct_products = df_raw[['sku', 'product_name', 'category']].drop_duplicates(subset=['sku'])
        
        for _, row in distinct_products.iterrows():
            conn.execute(
                text("""
                    INSERT OR IGNORE INTO dim_products (sku, product_name, category)
                    VALUES (:sku, :name, :cat);
                """),
                {"sku": row['sku'], "name": row['product_name'], "cat": row['category']}
            )
            
        # Map generated surrogate primary keys back to transactional metrics
        db_products = pd.read_sql("SELECT product_key, sku FROM dim_products", conn)
        df_fact_ready = df_raw.merge(db_products, on='sku', how='inner')
        
        # Isolate columns destined for the fact table
        fact_payload = df_fact_ready[[
            'snapshot_date', 'product_key', 'quantity_on_hand', 'unit_cost_usd', 'total_inventory_value'
        ]].rename(columns={'snapshot_date': 'date_actual'})
        
        print(f"📦 Streaming {len(fact_payload)} validated rows into fact_inventory...")
        fact_payload.to_sql('fact_inventory', con=conn, if_exists='append', index=False)
        
    print("✅ Local ETL Execution Completed Successfully.")

if __name__ == "__main__":
    run_etl_pipeline("wms_inventory_dump.csv")
