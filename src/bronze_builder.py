"""
Bronze Layer Builder (Ultra-Simple, Memory-Safe Version)
Direct-to-disk writing. NO pd.concat(). NO OOM errors.
"""

import os
import glob
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import gc

print("🏗️  Bronze Builder Starting...")

raw_dir = "/app/data/raw"
bronze_dir = "/app/data/bronze"
os.makedirs(bronze_dir, exist_ok=True)

# Get all 7 CSV files
files = sorted(glob.glob(os.path.join(raw_dir, "FY2021_All_Contracts_Full_*.csv")))
print(f"🔍 Found {len(files)} CSV files to process")

final_parquet_path = os.path.join(bronze_dir, "bronze_contracts.parquet")
metadata_path = os.path.join(bronze_dir, "bronze_metadata.json")

writer = None
total_rows = 0
global_row_id = 0

metadata = {
    "build_timestamp": datetime.now().isoformat(),
    "files_processed": [],
    "total_rows": 0,
    "total_columns": 0,
    "design_decision": "dtype=str preserves leading zeros. Direct-to-disk write prevents OOM."
}

# Process each file
for csv_file in files:
    file_name = os.path.basename(csv_file)
    print(f"\n{'='*60}")
    print(f"Processing: {file_name}")
    print(f"{'='*60}")
    
    file_rows = 0
    
    # CRITICAL: dtype=str protects leading zeros. chunksize=100000 protects RAM.
    for chunk in pd.read_csv(csv_file, dtype=str, chunksize=100000, low_memory=False):
        
        # Add traceability metadata
        chunk['_source_file'] = file_name
        chunk['_ingestion_timestamp'] = datetime.now().isoformat()
        chunk['_bronze_row_id'] = range(global_row_id, global_row_id + len(chunk))
        
        global_row_id += len(chunk)
        file_rows += len(chunk)
        
        # Convert Pandas chunk to PyArrow Table
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        
        # Initialize Parquet writer on the very first chunk
        if writer is None:
            writer = pq.ParquetWriter(final_parquet_path, table.schema, compression='snappy')
        
        # WRITE DIRECTLY TO DISK (This is the magic that prevents OOM)
        writer.write_table(table)
        
        # Immediately free RAM
        del chunk, table
        gc.collect()
        
    print(f"✅ Successfully wrote {file_rows:,} rows for {file_name}")
    metadata["files_processed"].append({"file_name": file_name, "rows": file_rows})
    total_rows += file_rows

# Close the writer properly
if writer is not None:
    writer.close()

# Get final column count from the saved file
pf = pq.ParquetFile(final_parquet_path)
final_col_count = len(pf.schema_arrow)

metadata["total_rows"] = total_rows
metadata["total_columns"] = final_col_count

# Save metadata JSON
with open(metadata_path, 'w') as f:
    json.dump(metadata, f, indent=2)

# Print Final Stats
file_size_mb = os.path.getsize(final_parquet_path) / (1024 * 1024)

print(f"\n{'='*60}")
print(f"✅ BRONZE LAYER BUILD COMPLETE")
print(f"{'='*60}")
print(f"Total rows:    {total_rows:,}")
print(f"Total columns: {final_col_count} (284 source + 3 metadata)")
print(f"File size:     {file_size_mb:.2f} MB")
print(f"Output:        {final_parquet_path}")
print(f"{'='*60}")