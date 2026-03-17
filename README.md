# Argentina Sailed — Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Status](https://img.shields.io/badge/status-active-success)
![Tests](https://img.shields.io/badge/tests-passing-brightgreen)
![License](https://img.shields.io/badge/license-private-lightgrey)

Automated data pipeline for Argentina grain shipment tracking, responsible for downloading, processing, merging, and persisting shipment data across multiple storage layers.

---

## Overview

This pipeline ensures data consistency and idempotency when handling monthly shipment updates, even when partial or overlapping datasets are ingested.

Key features:
- Incremental and safe monthly merge logic
- Multi-destination persistence (Excel, OneDrive, SQL Server)
- Automated logging with rotation
- Fully testable merge layer

---

## Project Structure
argentina_sailed/
├── main.py # Orchestrator — entry point
├── src/
│ ├── config.py # Environment configuration (.env)
│ ├── logger_config.py # Logging setup
│ ├── downloader.py # HTTP data downloader
│ ├── latest_file.py # Latest backup file resolver
│ └── database.py # Core logic (transform + merge + persist)
├── tests/
│ └── test_database.py # Unit tests (merge logic)
├── .env.example # Environment template
├── .env # Local config (ignored)
├── .gitignore
└── requirements.txt


---

## Pipeline Flow

    ┌───────────────┐
    │ Sailed Data   │
    └──────┬────────┘
           │
    ┌──────▼────────┐
    │ Line-Up Data  │
    └──────┬────────┘
           │
    ┌──────▼──────────────┐
    │ Latest File Resolver│
    └──────┬──────────────┘
           │
    ┌──────▼──────────────┐
    │ Existing Database   │
    └──────┬──────────────┘
           │
    ┌──────▼──────────────┐
    │ merge_com_banco()   │
    └──────┬──────────────┘
           │

┌───────────┼───────────────┐
▼ ▼ ▼
Local Excel OneDrive SQL Server
+ Pivot Tables


---

## Merge Strategy

The pipeline is designed to handle partial and multi-month updates safely.

### How it works

- The incoming dataset may contain multiple months  
  (e.g., Jan + Feb + partial Mar 2026)

- The function `merge_com_banco()`:
  1. Detects all `(month, year)` combinations in the new data
  2. Removes matching periods from the existing database
  3. Inserts the new records

### Guarantees

- No duplicated data (idempotent execution)
- Historical data is preserved
- Supports manual multi-month corrections
- Safe to run multiple times per day

---

## Setup

```bash
# Clone repository
git clone <your-repo-url>
cd argentina_sailed

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Fill in credentials and paths

# Run pipeline
python main.py
Tests
pytest tests/ -v

Focus:

Merge correctness

Edge cases (partial months, overlaps, reprocessing)

Outputs
Destination	Description
Local Excel	Arg_sailed_database_AT.xlsx (sheet data_base)
OneDrive	Full dataset + yearly sheets + pivot tables
SQL Server	[dbo].[Arg_Sailed] fully updated
Logging

Path:

C:\Users\server\Desktop\Argentina\logs\argentina_updater.log

Rotation:

5 MB per file

3 backup files retained

Environment Variables

Configured via .env file:

Example:

DB_CONNECTION_STRING=...
ONEDRIVE_PATH=...
DOWNLOAD_URL=...
Future Improvements

Docker containerization

CI/CD pipeline (GitHub Actions)

Data validation layer (schema enforcement)

Monitoring and alerting (Slack / Email)

Cloud migration (AWS / Azure)

Author

Eduardo Diamandis