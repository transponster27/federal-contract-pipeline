# Federal Contract Data Pipeline

A production-grade data pipeline for processing US Federal Contract data with memory-optimized architecture.

## Features

- Chunked data processing for large datasets
- Docker containerization with memory limits
- Multi-layer data architecture (Raw → Bronze → Silver → Quarantine)
- Data quality validation and reconciliation
- Type-safe data preservation (identifiers, financial values)

## Tech Stack

- Python 3.11
- Pandas
- PyArrow
- Docker & Docker Compose

## Project Structure
phase1-docker/
├── src/
│ ├── bronze_builder.py # Bronze layer construction
│ ├── silver_builder.py # Silver layer construction
│ ├── memory_monitor.py # Memory tracking
│ └── utils.py # Utility functions
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md


## Setup

1. Install Docker Desktop
2. Place source data files in `data/raw/`
3. Run: `docker compose up --build`

## Architecture

The pipeline follows a medallion architecture:
- **Bronze**: Raw source data preservation
- **Silver**: Cleaned and validated data
- **Quarantine**: Invalid records with failure reasons

## Note

This is a proprietary project. Source data is not included in this repository.