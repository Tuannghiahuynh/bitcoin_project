# Bitcoin ETL Project

A comprehensive data pipeline project to extract Bitcoin price data from Binance API, transform it, and load it into PostgreSQL database with automatic data archival and retention management.

## Project Structure

```
bitcoin_etl_project/
│
├── README.md
├── requirements.txt
├── .env                      # Database credentials (not tracked in git)
├── .gitignore
│
├── config/
│   ├── config.yaml          # Application configuration (API, ETL settings)
│   └── logging.yaml         # Logging configuration (not used - configured in code)
│
├── src/
│   ├── __init__.py
│   ├── main.py              # Main ETL pipeline entry point
│   │
│   ├── extract/
│   │   ├── __init__.py
│   │   └── bitcoin_api.py   # Extract data from Binance API
│   │
│   ├── transform/
│   │   ├── __init__.py
│   │   └── transform_data.py # Transform and validate data
│   │
│   ├── load/
│   │   ├── __init__.py
│   │   └── load_to_db.py    # Load data to PostgreSQL with CDC
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py        # Configuration manager
│   │   ├── db_connection.py # Database connection manager
│   │   ├── file_storage.py  # File storage and retention manager
│   │   └── logger.py        # Logging configuration
│
├── tests/
│
└── data/
    ├── raw/                 # Raw JSON files (7-day retention)
    ├── processed/           # Processed Parquet files (30-day retention)
    └── logs/                # Application logs (30-day rotation)
```

## Features

### Core ETL Pipeline
- **Extract**: Fetch Bitcoin (BTC/USDT) price data from Binance API
  - Configurable time intervals (default: 30 minutes)
  - Configurable data range (default: 120 days, max ~20 days per fetch)
  
- **Transform**: Clean and enrich data using pandas
  - Remove duplicates and invalid records
  - Add date and hour features
  - Calculate price statistics
  
- **Load**: Insert data into PostgreSQL database
  - Change Data Capture (CDC) - only insert new records
  - Conflict handling with upsert
  - Indexed for query performance

### Data Management
- **Raw Data Storage**: Save API responses as formatted JSON files
  - Metadata (source, symbol, fetch time)
  - Human-readable timestamps
  - OHLCV data (Open, High, Low, Close, Volume)
  - Auto-delete files older than 7 days
  
- **Processed Data Storage**: Save transformed data as Parquet files
  - Efficient columnar storage format
  - Daily aggregation with append logic
  - Auto-delete files older than 30 days

### Logging
- **Dual Output**: Console (detailed) + File (important only)
- **Smart Filtering**: File logs only capture critical events
- **Daily Rotation**: New log file each day, kept for 30 days

### Configuration
- **Centralized Config**: Single source of truth in `config.yaml`
- **Environment Variables**: Secure credential storage in `.env`
- **Easy Customization**: Change settings without modifying code

## Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd bitcoin_project
```

2. **Create virtual environment**:
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**:
   
   Create `.env` file in project root:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bitcoin_db
DB_USER=postgres
DB_PASSWORD=your_password
```

5. **Configure application settings** (optional):
   
   Edit `config/config.yaml` to customize:
   - API settings (symbol, interval, limit)
   - ETL parameters (fetch days, batch size)

## Database Setup

The pipeline requires a PostgreSQL database. The table will be created automatically on first run.

**Table Schema**:
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

## Usage

### Run ETL Pipeline

```bash
python src/main.py
```

The pipeline will:
1. Clean up old files (>7 days raw, >30 days processed)
2. Extract Bitcoin price data from Binance API
3. Save raw data as JSON in `data/raw/`
4. Transform and validate data
5. Save processed data as Parquet in `data/processed/`
6. Load new records into PostgreSQL database
7. Display summary statistics

### Scheduling (Optional)

Run the pipeline periodically using Windows Task Scheduler or cron:

**Windows Task Scheduler**:
- Action: `D:\bitcoin_project\.venv\Scripts\python.exe`
- Arguments: `D:\bitcoin_project\src\main.py`
- Start in: `D:\bitcoin_project`

**Linux/Mac cron** (every 30 minutes):
```bash
*/30 * * * * cd /path/to/bitcoin_project && .venv/bin/python src/main.py
```

## Configuration Files

### `.env` - Database Credentials
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bitcoin_db
DB_USER=postgres
DB_PASSWORD=postgres
```

### `config/config.yaml` - Application Settings
```yaml
api:
  binance:
    base_url: https://api.binance.com/api/v3
    symbol: BTCUSDT
    interval: 30m
    limit: 1000

etl:
  fetch_days: 120
  batch_size: 500
```

## Data Files

### Raw Data (JSON)
- Location: `data/raw/`
- Format: `bitcoin_raw_YYYYMMDD_HHMMSS.json`
- Retention: 7 days
- Structure: Metadata + OHLCV data with readable timestamps

### Processed Data (Parquet)
- Location: `data/processed/`
- Format: `bitcoin_processed_YYYYMMDD.parquet`
- Retention: 30 days
- Structure: Cleaned DataFrame with features (date, hour)

### Logs
- Location: `data/logs/`
- Format: `bitcoin_etl.log` (rotates daily)
- Retention: 30 days
- Content: Important events, statistics, errors

## Requirements

- Python 3.8+
- PostgreSQL 12+
- Libraries:
  - requests: API calls
  - pandas: Data manipulation
  - psycopg2-binary: PostgreSQL connection
  - python-dotenv: Environment variables
  - pyyaml: Configuration files
  - pyarrow: Parquet file support

## Monitoring

Check logs for pipeline status:
```bash
# View latest logs
tail -f data/logs/bitcoin_etl.log

# View today's log
cat data/logs/bitcoin_etl.log
```

## Troubleshooting

**Connection Error**:
- Verify PostgreSQL is running
- Check `.env` credentials
- Ensure database `bitcoin_db` exists

**Import Error**:
- Ensure virtual environment is activated
- Run: `pip install -r requirements.txt`

**API Rate Limit**:
- Binance has rate limits
- Don't run too frequently (recommended: 30 min intervals)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

MIT License

## Author

Your Name

## Acknowledgments

- Binance API for cryptocurrency data
- PostgreSQL for robust data storage
