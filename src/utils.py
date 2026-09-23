"""
Utility functions for data processing
"""
import pandas as pd
import numpy as np
from typing import List, Dict

# Important columns for Phase 1
IMPORTANT_COLUMNS = [
    'contract_award_unique_key', 'award_id_piid',
    'recipient_uei', 'recipient_duns', 'recipient_name',
    'recipient_address_line_1', 'recipient_address_line_2',
    'recipient_city_name', 'recipient_state_code', 'recipient_zip_4_code',
    'federal_action_obligation', 'total_dollars_obligated',
    'action_date', 'award_type', 'naics_code', 'product_or_service_code',
    'funding_agency_name', 'awarding_agency_name',
    'modification_number', 'award_description'
]

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize data types to reduce memory usage"""
    
    # Downcast integers
    for col in df.select_dtypes(include=['int64']).columns:
        col_min = df[col].min()
        col_max = df[col].max()
        
        if col_min >= np.iinfo(np.int32).min and col_max <= np.iinfo(np.int32).max:
            df[col] = df[col].astype(np.int32)
        
        if col_min >= np.iinfo(np.int16).min and col_max <= np.iinfo(np.int16).max:
            df[col] = df[col].astype(np.int16)
    
    # Downcast floats
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    
    # Convert low-cardinality strings to categorical
    for col in df.select_dtypes(include=['object']).columns:
        if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique
            df[col] = df[col].astype('category')
    
    return df

def load_csv_chunked(file_path: str, chunk_size: int = 100000) -> pd.DataFrame:
    """Load CSV in chunks to manage memory"""
    
    chunks = []
    
    # FIX: Removed engine='pyarrow' and dtype_backend='pyarrow' 
    # because they are not compatible with chunksize. 
    # The default C-engine is highly optimized for chunked reading.
    for chunk in pd.read_csv(
        file_path,
        chunksize=chunk_size,
        usecols=IMPORTANT_COLUMNS,  # Only load the 20 columns we need
        low_memory=False
    ):
        # Optimize data types for THIS chunk before appending
        # This is where the massive memory saving happens!
        chunk = optimize_dtypes(chunk)
        chunks.append(chunk)
    
    # Combine all optimized chunks into one dataframe
    df = pd.concat(chunks, ignore_index=True)
    
    return df

def get_file_list(data_dir: str) -> List[str]:
    """Get list of all CSV files in directory"""
    import os
    import glob
    
    pattern = os.path.join(data_dir, "FY2021_All_Contracts_Full_*.csv")
    files = sorted(glob.glob(pattern))
    
    return files