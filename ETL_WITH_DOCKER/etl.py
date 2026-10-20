import pandas as pd
import os
import time

def execute_pipeline():
    print("[ETL WORKER] Pipeline execution started...")
    
    raw_path = "data/raw_transactions.csv"
    summary_path = "data/summary_metrics.csv"
    
    # Check if the generated raw file exists in the shared folder
    if not os.path.exists(raw_path):
        print(f"[ETL ERROR] {raw_path} not found. Ensure generate_csv.py was run locally.")
        return

    # 1. EXTRACT: Read the heavy 5,000-row CSV file
    print("[ETL WORKER] Step 1/3: Reading raw transactional CSV...")
    df = pd.read_csv(raw_path)
    
    # 2. TRANSFORM: Filter out failed states and compute revenue aggregates per category
    print("[ETL WORKER] Step 2/3: Transforming data frameworks...")
    # Drop rows where payment failed
    successful_txns = df[df['payment_status'] == 'completed']
    
    # Aggregate sums and counts per target product vertical
    summary = successful_txns.groupby('product_category').agg(
        total_revenue=('amount', 'sum'),
        successful_transactions=('transaction_id', 'count')
    ).reset_index()
    
    # Calculate an additional helpful metric like average order value (AOV)
    summary['average_order_value'] = (summary['total_revenue'] / summary['successful_transactions']).round(2)
    summary['total_revenue'] = summary['total_revenue'].round(2)
    
    # 3. LOAD: Write the small aggregated footprint file back to the shared volume
    print("[ETL WORKER] Step 3/3: Exporting aggregated data metrics...")
    summary.to_csv(summary_path, index=False)
    
    print("[ETL WORKER] Pipeline process finished successfully!")

if __name__ == "__main__":
    execute_pipeline()
