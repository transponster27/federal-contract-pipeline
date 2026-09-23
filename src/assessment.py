"""
Phase 1 Assessment: Process all 7 files with memory optimization
"""
import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List

from memory_monitor import MemoryMonitor
from utils import IMPORTANT_COLUMNS, load_csv_chunked, get_file_list, optimize_dtypes

class Phase1Assessment:
    def __init__(self, data_dir: str = "/app/data/raw", output_dir: str = "/app/data/output"):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.monitor = MemoryMonitor()
        self.results = {
            "metadata": {
                "assessment_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "files_processed": [],
                "total_rows": 0
            },
            "file_summaries": [],
            "aggregate_metrics": {}
        }
        
    def assess_single_file(self, file_path: str) -> Dict:
        """Assess a single CSV file"""
        
        file_name = os.path.basename(file_path)
        print(f"\n{'='*60}")
        print(f"Processing: {file_name}")
        print(f"{'='*60}")
        
        self.monitor.log_memory(f"Starting {file_name}")
        
        # Load file in chunks
        df = load_csv_chunked(file_path, chunk_size=100000)
        
        self.monitor.log_memory(f"Loaded {file_name}")
        
        # Basic metrics
        file_summary = {
            "file_name": file_name,
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "memory_mb": df.memory_usage(deep=True).sum() / (1024**2)
        }
        
        # Data quality metrics
        quality_metrics = {
            "exact_duplicates": int(df.duplicated().sum()),
            "null_counts": {},
            "unique_counts": {}
        }
        
        for col in IMPORTANT_COLUMNS:
            if col in df.columns:
                quality_metrics["null_counts"][col] = int(df[col].isna().sum())
                quality_metrics["unique_counts"][col] = int(df[col].nunique())
        
        file_summary["quality_metrics"] = quality_metrics
        
        # Data type assessment
        dtype_issues = []
        for col in IMPORTANT_COLUMNS:
            if col in df.columns:
                if col in ['recipient_duns', 'naics_code'] and df[col].dtype in ['int64', 'float64']:
                    dtype_issues.append(f"{col}: Stored as numeric, should be string")
        
        file_summary["dtype_issues"] = dtype_issues
        
        # Financial metrics
        if 'federal_action_obligation' in df.columns:
            negative_count = int((df['federal_action_obligation'] < 0).sum())
            file_summary["negative_obligations"] = negative_count
        
        # Clean up
        del df
        import gc
        gc.collect()
        
        self.monitor.log_memory(f"Completed {file_name}")
        
        return file_summary
    
    def assess_all_files(self):
        """Assess all 7 CSV files"""
        
        files = get_file_list(self.data_dir)
        
        print(f"\n🔍 Found {len(files)} files to process")
        print(f"Files: {[os.path.basename(f) for f in files]}")
        
        for file_path in files:
            # Check memory before processing
            if not self.monitor.check_memory_limit(3500):
                print("⚠️  Memory limit approaching, forcing garbage collection")
                import gc
                gc.collect()
            
            # Process file
            file_summary = self.assess_single_file(file_path)
            
            # Add to results
            self.results["file_summaries"].append(file_summary)
            self.results["metadata"]["files_processed"].append(os.path.basename(file_path))
            self.results["metadata"]["total_rows"] += file_summary["total_rows"]
        
        # Calculate aggregate metrics
        self._calculate_aggregate_metrics()
        
        # Save results
        self._save_results()
    
    def _calculate_aggregate_metrics(self):
        """Calculate aggregate metrics across all files"""
        
        total_rows = sum(f["total_rows"] for f in self.results["file_summaries"])
        total_duplicates = sum(f["quality_metrics"]["exact_duplicates"] for f in self.results["file_summaries"])
        total_negative = sum(f.get("negative_obligations", 0) for f in self.results["file_summaries"])
        
        self.results["aggregate_metrics"] = {
            "total_rows": total_rows,
            "total_duplicate_rows": total_duplicates,
            "total_negative_obligations": total_negative,
            "files_count": len(self.results["file_summaries"])
        }
    
    def _save_results(self):
        """Save results to JSON"""
        
        output_file = os.path.join(self.output_dir, "phase1_assessment_all_files.json")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Results saved to: {output_file}")
        
        # Print summary
        print(f"\n{'='*60}")
        print("ASSESSMENT SUMMARY")
        print(f"{'='*60}")
        print(f"Files processed: {self.results['aggregate_metrics']['files_count']}")
        print(f"Total rows: {self.results['aggregate_metrics']['total_rows']:,}")
        print(f"Total duplicates: {self.results['aggregate_metrics']['total_duplicate_rows']:,}")
        print(f"Total negative obligations: {self.results['aggregate_metrics']['total_negative_obligations']:,}")
        print(f"Peak memory: {self.monitor.peak_memory:.2f} MB")
        print(f"{'='*60}")

if __name__ == "__main__":
    # Initialize assessment
    assessment = Phase1Assessment()
    
    # Run assessment
    assessment.assess_all_files()