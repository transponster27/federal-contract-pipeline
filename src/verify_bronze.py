"""
Verification script: Ensure Bronze layer is correct
"""
import pandas as pd
import os

bronze_file = "/app/data/bronze/bronze_contracts.parquet"

# Load Bronze
df = pd.read_parquet(bronze_file)

print(f"✅ Bronze Layer Verification")
print(f"{'='*60}")
print(f"Total rows: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"File size: {os.path.getsize(bronze_file) / (1024*1024):.2f} MB")

# Check 1: All 284 original columns + 3 metadata columns = 287
expected_columns = 284 + 3  # 284 original + _source_file, _ingestion_timestamp, _chunk_id
assert len(df.columns) == expected_columns, f"Expected {expected_columns} columns, got {len(df.columns)}"
print(f"✅ Column count correct: {len(df.columns)}")

# Check 2: Technical metadata exists
assert '_source_file' in df.columns
assert '_ingestion_timestamp' in df.columns
assert '_chunk_id' in df.columns
print(f"✅ Technical metadata columns present")

# Check 3: Total rows match Phase 1 assessment (6,364,766)
assert len(df) == 6364766, f"Expected 6,364,766 rows, got {len(df)}"
print(f"✅ Row count matches Phase 1: {len(df):,}")

# Check 4: Sample some columns to ensure data preserved
print(f"\n📊 Sample Data:")
print(f"   recipient_uei unique: {df['recipient_uei'].nunique():,}")
print(f"   federal_action_obligation sum: ${df['federal_action_obligation'].sum():,.2f}")
print(f"   Unique source files: {df['_source_file'].nunique()}")

print(f"\n{'='*60}")
print(f"✅ ALL VERIFICATIONS PASSED")
print(f"{'='*60}")