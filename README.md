# Commodities Market Data Fetcher

A Python application for fetching 5+ years of historical commodity market data using Angel Broking's SmartAPI and storing it in ClickHouse for analysis.

## Features

- Fetch historical market data for multiple commodities from MCX and NCDEX exchanges
- Support for different time intervals (1min, 5min, 15min, 30min, 1hour, 1day)
- Efficient chunking of data requests to handle API limitations
- Optimized ClickHouse database schema for time-series data analysis
- Command-line interface for flexible data fetching
- Data coverage reporting and checking

## Prerequisites

- Python 3.7 or above
- ClickHouse server installed and running
- SmartAPI credentials (API key, client code, password, TOTP secret)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/commodities-data-fetcher.git
cd commodities-data-fetcher
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your SmartAPI credentials as environment variables:
```bash
export SMARTAPI_KEY="your_api_key"
export SMARTAPI_CLIENT_CODE="your_client_code"
export SMARTAPI_PASSWORD="your_password"
export SMARTAPI_TOTP_KEY="your_totp_secret"
```

4. Set up ClickHouse connection details as environment variables (optional, default values provided):
```bash
export CLICKHOUSE_HOST="localhost"
export CLICKHOUSE_PORT="9000"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="your_password"
export CLICKHOUSE_DATABASE="market_data"
```

## Usage

### Setting up the database

Create the ClickHouse database and tables:

```bash
python scripts/create_tables.py
```

Or use the main script with the `--setup-db` flag:

```bash
python scripts/fetch_data.py --setup-db
```

### Listing available commodities

To see the list of available commodities:

```bash
python scripts/fetch_data.py --list-commodities
```

### Checking data coverage

To check the coverage of existing data in the database:

```bash
python scripts/fetch_data.py --check-coverage
```

### Fetching data

Fetch daily data for all commodities for the last 5+ years:

```bash
python scripts/fetch_data.py
```

Fetch data for a specific commodity:

```bash
python scripts/fetch_data.py --commodity GOLD
```

Fetch data for a specific exchange:

```bash
python scripts/fetch_data.py --exchange MCX_FO
```

Fetch data for a specific time interval:

```bash
python scripts/fetch_data.py --interval 1hour
```

Fetch data for a specific date range:

```bash
python scripts/fetch_data.py --start-date 2018-01-01 --end-date 2023-12-31
```

Force fetch data even if it already exists in the database:

```bash
python scripts/fetch_data.py --force-fetch
```

## Database Schema

The data is stored in two ClickHouse tables:

### commodity_market_data

Main table containing all market data at various intervals:

- `timestamp`: DateTime - Timestamp of the candle
- `exchange`: String - Exchange name
- `symbol_token`: String - Symbol token
- `interval`: String - Data interval
- `open`: Float64 - Opening price
- `high`: Float64 - Highest price during the interval
- `low`: Float64 - Lowest price during the interval
- `close`: Float64 - Closing price
- `volume`: Int64 - Volume traded

### commodity_daily_summary

Pre-aggregated daily summary table for faster queries:

- `date`: Date - Date of the trading day
- `exchange`: String - Exchange name
- `symbol_token`: String - Symbol token
- `open`: Float64 - Opening price
- `high`: Float64 - Highest price of the day
- `low`: Float64 - Lowest price of the day
- `close`: Float64 - Closing price
- `volume`: Int64 - Volume traded
- `year`: UInt16 - Year (for efficient filtering)
- `month`: UInt8 - Month (for efficient filtering)

## Sample Queries

### Basic price query

```sql
SELECT 
    timestamp, 
    open, 
    high, 
    low, 
    close 
FROM commodity_market_data 
WHERE 
    exchange = 'MCX_FO' 
    AND symbol_token = '234230' -- GOLD
    AND interval = 'ONE_DAY'
    AND timestamp BETWEEN '2022-01-01 00:00:00' AND '2022-12-31 23:59:59'
ORDER BY timestamp
```

### Daily price query

```sql
SELECT 
    date, 
    open, 
    high, 
    low, 
    close 
FROM commodity_daily_summary 
WHERE 
    exchange = 'MCX_FO' 
    AND symbol_token = '234230' -- GOLD
    AND date BETWEEN '2022-01-01' AND '2022-12-31'
ORDER BY date
```

### Monthly average prices

```sql
SELECT 
    toStartOfMonth(date) AS month, 
    avg(close) AS avg_price 
FROM commodity_daily_summary 
WHERE 
    exchange = 'MCX_FO' 
    AND symbol_token = '234230' -- GOLD
    AND year = 2022
GROUP BY month
ORDER BY month
```

## Project Structure

```
commodities-data-fetcher/
├── config/
│   ├── __init__.py
│   └── settings.py            # Configuration parameters
├── src/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── smart_api_client.py # SmartAPI wrapper
│   ├── db/
│   │   ├── __init__.py
│   │   └── clickhouse_client.py # ClickHouse connector
│   ├── models/
│   │   ├── __init__.py
│   │   └── market_data.py       # Data models
│   └── utils/
│       ├── __init__.py
│       └── helpers.py          # Helper functions
├── scripts/
│   ├── __init__.py
│   ├── fetch_data.py          # Main data fetching script
│   └── create_tables.py       # DB setup script
├── logs/                      # Log directory
├── README.md
├── requirements.txt
└── setup.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- Angel Broking for providing SmartAPI
- ClickHouse team for their excellent database