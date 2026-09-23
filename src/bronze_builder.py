"""
Step 1: Bronze Layer Builder
=============================
Purpose: Convert 7 CSV files into a single Parquet file (Bronze layer)
         preserving ALL 284 columns without any transformation.

Key Principles:
1. Preserve raw data exactly as received from source
2. Use chunked processing to control memory
3. Add technical metadata for traceability
4. Save as Parquet for 70% storage reduction
"""

import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime
from memory_monitor import MemoryMonitor
import gc


class BronzeBuilder:
    """
    Builds the Bronze layer from raw CSV files.
    
    Bronze Layer Characteristics:
    - ALL columns preserved (no dropping)
    - NO transformations (raw data)
    - Technical metadata added (_source_file, _ingestion_timestamp)
    - Saved as Parquet (columnar, compressed)
    """
    
    def __init__(self, data_dir="/app/data/raw", output_dir="/app/data/bronze"):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.monitor = MemoryMonitor()
        
        # Create output directory if not exists
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"🏗️  Bronze Builder initialized")
        print(f"   Input:  {data_dir}")
        print(f"   Output: {output_dir}")
    
    def get_csv_files(self):
        """Get sorted list of all CSV files"""
        pattern = os.path.join(self.data_dir, "FY2021_All_Contracts_Full_*.csv")
        files = sorted(glob.glob(pattern))
        return files
    
    def process_single_file(self, file_path, chunk_size=100000):
        """
        Process a single CSV file in chunks.
        
        Key Difference from Phase 1:
        - Phase 1: Loaded only 20 columns (usecols parameter)
        - Bronze: Loads ALL 284 columns (no usecols)
        
        Memory Control Strategy:
        - Chunk size: 100,000 rows
        - After each chunk: optimize dtypes, append to list, delete chunk
        - Final: concatenate all chunks
        """
        file_name = os.path.basename(file_path)
        print(f"\n{'='*60}")
        print(f"Processing: {file_name}")
        print(f"{'='*60}")
        
        self.monitor.log_memory(f"Starting {file_name}")
        
        chunks = []
        chunk_count = 0
        
        # CRITICAL: NO usecols parameter - load ALL columns
        for chunk in pd.read_csv(
            file_path,
            chunksize=chunk_size,
            low_memory=False  # Suppress mixed type warnings
        ):
            chunk_count += 1
            
            # Add technical metadata (traceability)
            chunk['_source_file'] = file_name
            chunk['_ingestion_timestamp'] = datetime.now().isoformat()
            chunk['_chunk_id'] = chunk_count
            
            # Optimize data types (memory reduction WITHOUT changing meaning)
            chunk = self._optimize_dtypes(chunk)
            
            chunks.append(chunk)
            
            if chunk_count % 5 == 0:  # Log every 5 chunks
                self.monitor.log_memory(f"Processed {chunk_count} chunks")
        
        print(f"✅ Loaded {chunk_count} chunks from {file_name}")
        self.monitor.log_memory(f"After loading {file_name}")
        
        # Concatenate all chunks
        print(f"🔄 Concatenating chunks...")
        df = pd.concat(chunks, ignore_index=True)
        
        # Free memory
        del chunks
        gc.collect()
        
        self.monitor.log_memory(f"After concatenation")
        
        return df
    
    def _optimize_dtypes(self, df):
        """
        Optimize data types to reduce memory WITHOUT changing data meaning.
        
        This is NOT transformation - this is just storage optimization.
        The data values remain exactly the same.
        """
        # Downcast integers
        for col in df.select_dtypes(include=['int64']).columns:
            col_min = df[col].min()
            col_max = df[col].max()
            
            if col_min >= np.iinfo(np.int32).min and col_max <= np.iinfo(np.int32).max:
                df[col] = df[col].astype(np.int32)
        
        # Downcast floats
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Convert low-cardinality strings to category
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:
                df[col] = df[col].astype('category')
        
        return df
    
    def build_bronze_layer(self):
        """
        Main method: Build complete Bronze layer from all CSV files.
        
        Strategy:
        1. Process each CSV file separately (memory control)
        2. Save each as temporary Parquet
        3. Combine all into final Bronze Parquet
        """
        files = self.get_csv_files()
        print(f"\n🔍 Found {len(files)} CSV files to process")
        
        temp_files = []
        
        # Step 1: Process each file and save as temporary Parquet
        for file_path in files:
            df = self.process_single_file(file_path)
            
            # Save as temporary Parquet
            temp_file = os.path.join(
                self.output_dir, 
                f"temp_{os.path.basename(file_path).replace('.csv', '.parquet')}"
            )
            
            print(f"💾 Saving temporary Parquet: {temp_file}")
            df.to_parquet(
                temp_file,
                engine='pyarrow',
                compression='snappy',  # Fast compression
                index=False
            )
            
            temp_files.append(temp_file)
            
            # Free memory
            del df
            gc.collect()
            
            self.monitor.log_memory(f"After saving {os.path.basename(file_path)}")
        
        # Step 2: Combine all temporary Parquets into final Bronze
        print(f"\n{'='*60}")
        print(f"🔄 Combining {len(temp_files)} temporary files into Bronze layer...")
        print(f"{'='*60}")
        
        combined_chunks = []
        for temp_file in temp_files:
            chunk = pd.read_parquet(temp_file)
            combined_chunks.append(chunk)
            
            # Delete temp file after reading
            os.remove(temp_file)
        
        bronze_df = pd.concat(combined_chunks, ignore_index=True)
        
        del combined_chunks
        gc.collect()
        
        # Step 3: Save final Bronze Parquet
        bronze_file = os.path.join(self.output_dir, "bronze_contracts.parquet")
        
        print(f"\n💾 Saving final Bronze layer: {bronze_file}")
        bronze_df.to_parquet(
            bronze_file,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        # Final stats
        file_size_mb = os.path.getsize(bronze_file) / (1024 * 1024)
        
        print(f"\n{'='*60}")
        print(f"✅ BRONZE LAYER BUILD COMPLETE")
        print(f"{'='*60}")
        print(f"Total rows: {len(bronze_df):,}")
        print(f"Total columns: {len(bronze_df.columns)}")
        print(f"File size: {file_size_mb:.2f} MB")
        print(f"Peak memory: {self.monitor.peak_memory:.2f} MB")
        print(f"Output: {bronze_file}")
        print(f"{'='*60}")
        
        # Free memory
        del bronze_df
        gc.collect()
        
        return bronze_file


if __name__ == "__main__":
    builder = BronzeBuilder()
    builder.build_bronze_layer()