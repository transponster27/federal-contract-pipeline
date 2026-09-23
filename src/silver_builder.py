# silver_builder.py

import pandas as pd
import numpy as np
from datetime import datetime

def build_silver_layer():
    """
    Silver Layer: Clean and validate, but preserve transaction grain.
    - Load from Bronze
    - Apply cleaning rules (standardize dates, classify states, etc.)
    - Add flags (is_de_obligation, is_admin_modification)
    - DO NOT aggregate or change grain
    - Route invalid records to Quarantine
    """
    
    bronze_path = "data/bronze/bronze_contracts.parquet"
    silver_path = "data/silver/silver_contracts.parquet"
    quarantine_path = "data/quarantine/quarantine_records.parquet"
    
    print("Loading Bronze layer...")
    df = pd.read_parquet(bronze_path)
    
    print(f"Loaded {len(df):,} rows from Bronze")
    
    # Initialize quarantine list
    quarantine_records = []
    
    # ========================================
    # RULE 1: Validate required fields
    # ========================================
    print("\nApplying Rule 1: Required field validation...")
    
    # contract_transaction_unique_key must not be null
    null_transaction_key = df['contract_transaction_unique_key'].isna()
    if null_transaction_key.sum() > 0:
        print(f"  Found {null_transaction_key.sum()} rows with null transaction key")
        quarantine_records.extend(df[null_transaction_key].assign(
            _quarantine_reason="Missing contract_transaction_unique_key"
        ).to_dict('records'))
        df = df[~null_transaction_key]
    
    # ========================================
    # RULE 2: Standardize dates
    # ========================================
    print("\nApplying Rule 2: Date standardization...")
    
    # Convert action_date to proper datetime
    df['action_date_parsed'] = pd.to_datetime(df['action_date'], errors='coerce')
    
    # Check for invalid dates
    invalid_dates = df['action_date_parsed'].isna() & df['action_date'].notna()
    if invalid_dates.sum() > 0:
        print(f"  Found {invalid_dates.sum()} rows with invalid date format")
        quarantine_records.extend(df[invalid_dates].assign(
            _quarantine_reason="Invalid action_date format"
        ).to_dict('records'))
        df = df[~invalid_dates]
    
    # ========================================
    # RULE 3: Preserve financial values (no sign change)
    # ========================================
    print("\nApplying Rule 3: Financial value preservation...")
    
    # Convert to float (keep original sign)
    df['federal_action_obligation'] = pd.to_numeric(
        df['federal_action_obligation'], 
        errors='coerce'
    )
    
    # Add flag for de-obligations (but keep original value)
    df['is_de_obligation'] = df['federal_action_obligation'] < 0
    df['is_admin_modification'] = df['federal_action_obligation'] == 0
    
    print(f"  De-obligations: {df['is_de_obligation'].sum():,}")
    print(f"  Admin modifications (zero-dollar): {df['is_admin_modification'].sum():,}")
    
    # ========================================
    # RULE 4: Classify state codes
    # ========================================
    print("\nApplying Rule 4: State code classification...")
    
    # Define valid federal codes
    state_codes = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                   'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                   'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                   'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                   'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
    
    territory_codes = ['DC', 'PR', 'VI', 'GU', 'AS', 'MP']
    military_codes = ['AE', 'AA', 'AP']
    
    def classify_state(code):
        if pd.isna(code):
            return 'Unknown'
        elif code in state_codes:
            return 'State'
        elif code in territory_codes:
            return 'Territory'
        elif code in military_codes:
            return 'Military'
        else:
            return 'Invalid'
    
    df['state_classification'] = df['recipient_state_code'].apply(classify_state)
    
    print(f"  State distribution:")
    print(df['state_classification'].value_counts())
    
    # ========================================
    # RULE 5: Preserve identifiers as strings
    # ========================================
    print("\nApplying Rule 5: Identifier preservation...")
    
    # These columns must remain strings (already are from Bronze)
    identifier_columns = [
        'recipient_uei', 'recipient_duns', 'naics_code', 
        'recipient_zip_4_code', 'contract_award_unique_key',
        'contract_transaction_unique_key'
    ]
    
    # Verify they're still strings
    for col in identifier_columns:
        if df[col].dtype != 'object':
            print(f"  WARNING: {col} is not string, converting...")
            df[col] = df[col].astype(str)
    
    # ========================================
    # Save Silver and Quarantine
    # ========================================
    print("\nSaving Silver layer...")
    df.to_parquet(silver_path, compression='snappy')
    
    if quarantine_records:
        print(f"\nSaving {len(quarantine_records)} records to Quarantine...")
        quarantine_df = pd.DataFrame(quarantine_records)
        quarantine_df.to_parquet(quarantine_path, compression='snappy')
    
    print(f"\n✅ Silver layer built:")
    print(f"   Valid rows: {len(df):,}")
    print(f"   Quarantined rows: {len(quarantine_records):,}")
    print(f"   Grain preserved: 1 row = 1 transaction")
    print(f"   Saved to: {silver_path}")
    
    # Reconciliation check
    bronze_count = pd.read_parquet(bronze_path).shape[0]
    silver_count = len(df)
    quarantine_count = len(quarantine_records)
    
    print(f"\n📊 RECONCILIATION:")
    print(f"   Bronze rows: {bronze_count:,}")
    print(f"   Silver rows: {silver_count:,}")
    print(f"   Quarantine rows: {quarantine_count:,}")
    print(f"   Total: {silver_count + quarantine_count:,}")
    
    if bronze_count == silver_count + quarantine_count:
        print(f"   ✅ RECONCILED: No records lost")
    else:
        print(f"   ❌ MISMATCH: {bronze_count - (silver_count + quarantine_count)} records unaccounted")

if __name__ == "__main__":
    build_silver_layer()