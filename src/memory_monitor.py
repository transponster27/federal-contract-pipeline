"""
Memory Monitor: Tracks memory usage in real-time
"""
import psutil
import os
import time
from datetime import datetime

class MemoryMonitor:
    def __init__(self, process_name="python"):
        self.process = psutil.Process(os.getpid())
        self.start_time = time.time()
        self.peak_memory = 0
        
    def get_memory_usage(self):
        """Get current memory usage in MB"""
        memory_info = self.process.memory_info()
        current_mb = memory_info.rss / (1024 * 1024)
        
        # Track peak
        if current_mb > self.peak_memory:
            self.peak_memory = current_mb
            
        return current_mb
    
    def log_memory(self, operation=""):
        """Log current memory usage"""
        current_mb = self.get_memory_usage()
        elapsed = time.time() - self.start_time
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] "
              f"Memory: {current_mb:.2f} MB | "
              f"Peak: {self.peak_memory:.2f} MB | "
              f"Elapsed: {elapsed:.1f}s | "
              f"Operation: {operation}")
        
        return current_mb
    
    def check_memory_limit(self, limit_mb=3500):
        """Check if approaching memory limit"""
        current_mb = self.get_memory_usage()
        
        if current_mb > limit_mb:
            print(f"⚠️  WARNING: Memory usage {current_mb:.2f} MB exceeds limit {limit_mb} MB")
            return False
        
        return True
    
    def get_summary(self):
        """Get memory usage summary"""
        return {
            "current_mb": self.get_memory_usage(),
            "peak_mb": self.peak_memory,
            "elapsed_seconds": time.time() - self.start_time
        }