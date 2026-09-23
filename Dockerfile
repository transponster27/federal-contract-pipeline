# Use lightweight Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies (minimal)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for Docker layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/ ./src/

# Set environment variables for memory optimization, Reduces memory fragmentation, Real-time output logging
ENV PYTHONMALLOC=malloc
ENV MALLOC_ARENA_MAX=2 
ENV PYTHONUNBUFFERED=1

# Create data directories
RUN mkdir -p /app/data/raw /app/data/output

# Default command
CMD ["python", "src/assessment.py"]