import csv
import random
from datetime import datetime, timedelta

# Create a 'data' directory if it doesn't exist
import os
os.makedirs('data', exist_ok=True)

# Configurations for a large dataset
NUM_ROWS = 5000
CATEGORIES = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Beauty']
STATUSES = ['completed', 'pending', 'failed']

start_time = datetime(2026, 1, 1)

with open('data/raw_transactions.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # Header row
    writer.writerow(['transaction_id', 'timestamp', 'user_id', 'product_category', 'amount', 'payment_status'])
    
    for i in range(NUM_ROWS):
        txn_id = f"TXN_{100000 + i}"
        timestamp = (start_time + timedelta(minutes=i)).strftime('%Y-%m-%d %H:%M:%S')
        user_id = f"USR_{random.randint(1000, 9999)}"
        category = random.choice(CATEGORIES)
        amount = round(random.uniform(5.0, 750.0), 2)
        # 85% completed, 10% pending, 5% failed
        status = random.choices(STATUSES, weights=[0.85, 0.10, 0.05], k=1)[0]
        
        writer.writerow([txn_id, timestamp, user_id, category, amount, status])

print(f"Successfully generated 'data/raw_transactions.csv' with {NUM_ROWS} rows!")
