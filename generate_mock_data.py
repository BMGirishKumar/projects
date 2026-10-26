import os
import json
import uuid
import random
import time
from datetime import datetime, timedelta
from multiprocessing import Process

def generate_file_chunk(file_id, target_records):
    """Generates a single structural log file filled with mock WMS events."""
    event_types = ['PICK', 'PACK', 'STAGE', 'SHIP']
    zones = ['ZONE_A', 'ZONE_B', 'ZONE_C', 'ZONE_D']
    operators = [f"OP_{i:03d}" for i in range(1, 51)]
    
    start_time = datetime(2026, 1, 1)
    file_path = f"data/wms_events/wms_logs_chunk_{file_id}.json"
    
    print(f"📦 Worker starting chunk {file_id}... Writing {target_records:,} records.")
    
    with open(file_path, 'w') as f:
        for _ in range(target_records):
            # Create natural operational bias/skew (Zone C will be slower)
            chosen_zone = random.choice(zones)
            if chosen_zone == 'ZONE_C':
                processing_time = round(random.uniform(150, 450), 2) # Heavily skewed to induce delays
            else:
                processing_time = round(random.uniform(20, 200), 2)  # Fast processing
                
            record = {
                "event_id": str(uuid.uuid4()),
                "timestamp": (start_time + timedelta(seconds=random.randint(0, 2592000))).strftime('%Y-%m-%d %H:%M:%S'),
                "item_id": f"ITEM_{random.randint(100000, 999999)}",
                "zone_id": chosen_zone,
                "event_type": random.choice(event_types),
                "processing_time_sec": processing_time,
                "operator_id": random.choice(operators)
            }
            f.write(json.dumps(record) + '\n')
            
    print(f"✅ Chunk {file_id} written successfully.")

def build_wms_dataset(gb_target=5):
    """Coordinates multi-core execution to generate a high-volume dataset."""
    os.makedirs("data/wms_events", exist_ok=True)
    os.makedirs("data/output", exist_ok=True)
    
    # Crucial Calculation: 1 million lines of this JSON schema ≈ 165 MB.
    # Therefore, 5 GB requires ~30 million total operational rows.
    records_per_million = 30000000
    total_workers = 6 # Split the work across 6 parallel CPU subprocesses
    records_per_worker = records_per_million // total_workers
    
    print(f"🛠️ Starting high-throughput data generation engine...")
    print(f"📊 Target Volume: ~{gb_target} GB | Processes: {total_workers}")
    
    processes = []
    start_wall_time = time.time()
    
    for i in range(total_workers):
        p = Process(target=generate_file_chunk, args=(i, records_per_worker))
        processes.append(p)
        p.start()
        
    for p in processes:
        p.join()
        
    end_wall_time = time.time()
    print(f"\n🎉 Generation Complete in {round(end_wall_time - start_wall_time, 2)} seconds!")
    print(f"📁 Check the directory: 'data/wms_events/' to verify the files.")

if __name__ == "__main__":
    build_wms_dataset(gb_target=5)
