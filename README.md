# Bitcoin ETL Project

Personal data engineering project to extract Bitcoin price data from Binance API, transform it, and load it into PostgreSQL database with automated data management and visualization.

## Overview

This ETL pipeline fetches Bitcoin (BTC/USDT) price data every 30 minutes, processes it, stores it in PostgreSQL, and enables analysis through Power BI dashboards. The entire workflow can be orchestrated using Apache Airflow for automated scheduling.

## Features

### ETL Pipeline
- **Extract**: Fetch Bitcoin price data from Binance API (30-minute intervals)
- **Transform**: Clean, validate, and enrich data with pandas
- **Load**: Insert into PostgreSQL with Change Data Capture (CDC)

### Data Management
- **Raw Data**: JSON files with metadata (7-day retention)
- **Processed Data**: Parquet files for efficient storage (30-day retention)
- **Auto Cleanup**: Automatic removal of old files
- **Database**: PostgreSQL with indexed tables for fast queries

### Logging & Monitoring
- **Smart Logging**: Console (detailed) + File (important events only)
- **Daily Rotation**: Automatic log file rotation
- **Statistics**: Track records fetched, processed, and inserted

### Visualization
- **Power BI**: Connect to PostgreSQL for interactive dashboards
- **Analytics**: Price trends, volatility, time-based patterns

### Orchestration
- **Apache Airflow**: Schedule and monitor ETL jobs
- **DAG Support**: Ready for workflow automation

## Project Structure

```
bitcoin_project/
├── src/
│   ├── main.py                  # ETL pipeline entry point
│   ├── extract/                 # Data extraction from Binance API
│   ├── transform/               # Data transformation and validation
│   ├── load/                    # Load to PostgreSQL with CDC
│   └── utils/                   # Configuration, logging, file storage
├── config/
│   └── config.yaml              # Application configuration
├── data/
│   ├── raw/                     # Raw JSON files (7 days)
│   ├── processed/               # Parquet files (30 days)
│   └── logs/                    # Application logs
├── .env                         # Database credentials
├── requirements.txt
└── README.md
```

## Installation

1. **Clone repository**:
```bash
git clone https://github.com/Tuannghiahuynh/bitcoin_project.git
cd bitcoin_project
```

2. **Create virtual environment**:
```bash
python -m venv .venv
.venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure `.env`**:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bitcoin_db
DB_USER=postgres
DB_PASSWORD=your_password
```

5. **Create database**:
```sql
CREATE DATABASE bitcoin_db;
```

## Usage

### Run ETL Pipeline

```bash
python src/main.py
```

The pipeline will:
1. Clean up old files (>7 days raw, >30 days processed)
2. Fetch Bitcoin prices from Binance API
3. Save raw data as formatted JSON
4. Transform and save as Parquet
5. Load new records to PostgreSQL (CDC)
6. Display summary statistics

## Database Schema

```sql
CREATE TABLE bitcoin_prices (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE NOT NULL,
    price NUMERIC(15, 2) NOT NULL,
    date DATE,
    hour INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Configuration

### Application Settings (`config/config.yaml`)
```yaml
api:
  binance:
    symbol: BTCUSDT
    interval: 30m
    limit: 1000

etl:
  fetch_days: 120
  batch_size: 500
```

## Power BI Visualization

### Connect to PostgreSQL
1. Open Power BI Desktop
2. Get Data → PostgreSQL database
3. Server: `localhost:5432`
4. Database: `bitcoin_db`
5. Table: `bitcoin_prices`

### Suggested Visualizations
- **Line Chart**: Price over time
- **Area Chart**: Price volatility by hour
- **Card**: Current price, 24h change, min/max
- **Table**: Recent transactions
- **Slicers**: Filter by date range

### Key Metrics
- Current BTC price
- 24-hour price change (%)
- 7-day moving average
- Daily high/low
- Hourly price distribution

## Apache Airflow Orchestration

### Setup Airflow DAG

Create `dags/bitcoin_etl_dag.py`:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bitcoin_etl_pipeline',
    default_args=default_args,
    description='Bitcoin ETL Pipeline',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False
)

run_etl = BashOperator(
    task_id='run_bitcoin_etl',
    bash_command='cd /path/to/bitcoin_project && .venv/bin/python src/main.py',
    dag=dag
)
```

### Schedule
- **Recommended**: Every 30 minutes (matches data interval)
- **Alternative**: Hourly or custom schedule

## Data Files

### Raw JSON (`data/raw/`)
- Format: `bitcoin_raw_YYYYMMDD_HHMMSS.json`
- Content: API response with metadata, OHLCV data
- Retention: 7 days

### Processed Parquet (`data/processed/`)
- Format: `bitcoin_processed_YYYYMMDD.parquet`
- Content: Cleaned DataFrame with features
- Retention: 30 days
- Daily aggregation with automatic append

## Tech Stack

- **Python 3.8+**: Core programming language
- **PostgreSQL 12+**: Data warehouse
- **Pandas**: Data transformation
- **Binance API**: Data source (free, no API key required)
- **Parquet**: Efficient columnar storage
- **Power BI**: Data visualization and dashboards
- **Apache Airflow**: Workflow orchestration (optional)

## Requirements

```
requests==2.31.0
pandas==2.1.4
psycopg2-binary==2.9.9
python-dotenv==1.0.0
pyyaml==6.0.1
pyarrow==14.0.1
```

## Monitoring

View logs:
```bash
tail -f data/logs/bitcoin_etl.log
```

Check file statistics:
- Raw files count and size
- Processed files count and size
- Database record count

## Author

Tuan Nghia Huynh
