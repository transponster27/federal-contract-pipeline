"""
Silver Layer Builder (Cleaning, Validation & Quarantine)
========================================================
Reads from Bronze, applies business rules, and writes to Silver & Quarantine.
Preserves transaction grain (1 row = 1 transaction).
"""

import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import gc

print("🥈 Silver Builder Starting...")

# Paths
bronze_path = "/app/data/bronze/bronze_contracts.parquet"
silver_dir = "/app/data/silver"
quarantine_dir = "/app/data/quarantine"

os.makedirs(silver_dir, exist_ok=True)
os.makedirs(quarantine_dir, exist_ok=True)

silver_path = os.path.join(silver_dir, "silver_contracts.parquet")
quarantine_path = os.path.join(quarantine_dir, "quarantine_records.parquet")

# Initialize PyArrow Writers
silver_writer = None
quarantine_writer = None

total_silver_rows = 0
total_quarantine_rows = 0

# State Code Classification Logic
STATE_CODES = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 
               'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
               'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 
               'VA', 'WA', 'WV', 'WI', 'WY']
TERRITORY_CODES = ['DC', 'PR', 'VI', 'GU', 'AS', 'MP']
MILITARY_CODES = ['AE', 'AA', 'AP']

def classify_state(code):
    if pd.isna(code) or str(code).strip() == '':
        return 'Unknown'
    code_str = str(code).strip().upper()
    if code_str in STATE_CODES:
        return 'State'
    elif code_str in TERRITORY_CODES:
        return 'Territory'
    elif code_str in MILITARY_CODES:
        return 'Military'
    else:
        return 'Invalid'

# Read Bronze in batches (Memory Safe)
print(f"📖 Reading Bronze layer from: {bronze_path}")
pf = pq.ParquetFile(bronze_path)

for batch in pf.iter_batches(batch_size=100000):
    # Convert PyArrow batch to Pandas DataFrame
    chunk = batch.to_pandas()
    
    # --- CLEANING & VALIDATION LOGIC ---
    
    # 1. Validate Required Fields (contract_transaction_unique_key must exist)
    invalid_mask = chunk['contract_transaction_unique_key'].isna() | (chunk['contract_transaction_unique_key'].astype(str).str.strip() == '')
    
    if invalid_mask.any():
        # Route invalid records to Quarantine
        q_chunk = chunk[invalid_mask].copy()
        q_chunk['_quarantine_reason'] = "Missing or empty contract_transaction_unique_key"
        
        q_table = pa.Table.from_pandas(q_chunk, preserve_index=False)
        if quarantine_writer is None:
            quarantine_writer = pq.ParquetWriter(quarantine_path, q_table.schema, compression='snappy')
        quarantine_writer.write_table(q_table)
        
        total_quarantine_rows += len(q_chunk)
        
        # Keep only valid records for Silver
        chunk = chunk[~invalid_mask]

    if len(chunk) > 0:
        # 2. Standardize Dates (Keep original, add parsed)
        chunk['action_date_parsed'] = pd.to_datetime(chunk['action_date'], errors='coerce')
        
        # Check for invalid dates and quarantine them
        invalid_dates = chunk['action_date_parsed'].isna() & chunk['action_date'].notna() & (chunk['action_date'].astype(str).str.strip() != '')
        if invalid_dates.any():
            q_chunk = chunk[invalid_dates].copy()
            q_chunk['_quarantine_reason'] = "Invalid action_date format"
            
            q_table = pa.Table.from_pandas(q_chunk, preserve_index=False)
            if quarantine_writer is None:
                quarantine_writer = pq.ParquetWriter(quarantine_path, q_table.schema, compression='snappy')
            quarantine_writer.write_table(q_table)
            
            total_quarantine_rows += len(q_chunk)
            chunk = chunk[~invalid_dates]

    if len(chunk) > 0:
        # 3. Financial Logic (Preserve sign, add flag)
        # Convert string to float for calculations, keep original string column intact
        chunk['obligation_amount_float'] = pd.to_numeric(chunk['federal_action_obligation'], errors='coerce')
        chunk['is_de_obligation'] = chunk['obligation_amount_float'] < 0
        chunk['is_admin_modification'] = chunk['obligation_amount_float'] == 0.0
        
        # 4. State Code Classification
        chunk['state_classification'] = chunk['recipient_state_code'].apply(classify_state)
        
        # 5. Write to Silver
        s_table = pa.Table.from_pandas(chunk, preserve_index=False)
        if silver_writer is None:
            silver_writer = pq.ParquetWriter(silver_path, s_table.schema, compression='snappy')
        silver_writer.write_table(s_table)
        
        total_silver_rows += len(chunk)
        
    # Free memory
    del chunk
    gc.collect()

# Close writers
if silver_writer: silver_writer.close()
if quarantine_writer: quarantine_writer.close()

# Final Stats
print(f"\n{'='*60}")
print(f"✅ SILVER LAYER BUILD COMPLETE")
print(f"{'='*60}")
print(f"Valid rows (Silver):       {total_silver_rows:,}")
print(f"Invalid rows (Quarantine): {total_quarantine_rows:,}")
print(f"Total processed:           {total_silver_rows + total_quarantine_rows:,}")

if os.path.exists(silver_path):
    print(f"Silver size: {os.path.getsize(silver_path) / (1024*1024):.2f} MB")
if os.path.exists(quarantine_path):
    print(f"Quarantine size: {os.path.getsize(quarantine_path) / (1024*1024):.2f} MB")

print(f"{'='*60}")