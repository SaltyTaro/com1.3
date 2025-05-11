"""
ClickHouse database client for storing commodity market data
"""
import logging
from typing import List, Dict, Optional, Any
import pandas as pd
from clickhouse_driver import Client

from config.settings import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, 
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE, BATCH_SIZE
)

logger = logging.getLogger(__name__)

class ClickHouseClient:
    """
    Client for interacting with ClickHouse database
    """
    def __init__(self):
        # Initially connect without specifying a database
        try:
            self.client = Client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                settings={
                    'use_numpy': True,
                    'connect_timeout': 10,  # Shorter connect timeout
                    'send_timeout': 30,     # Send timeout
                    'receive_timeout': 30,  # Receive timeout
                    'client_name': 'commodity_data_fetcher'
                }
            )
            logger.info(f"Successfully initialized ClickHouse client to {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse client: {str(e)}")
            raise
        
    def test_connection(self) -> bool:
        """
        Test the connection to ClickHouse
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            result = self.client.execute("SELECT 1")
            return result == [(1,)]
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {str(e)}")
            return False

    def create_database(self) -> bool:
        """
        Create the database if it does not exist
        
        Returns:
            bool: True if database creation is successful, False otherwise
        """
        try:
            # Create database if not exists
            self.client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
            
            # Now set the database for future queries
            self.client.execute(f"USE {CLICKHOUSE_DATABASE}")
            
            logger.info(f"Database '{CLICKHOUSE_DATABASE}' created or already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create database: {str(e)}")
            return False

    def create_market_data_table(self) -> bool:
        """
        Create the market data table with optimized schema for time-series data
        
        Returns:
            bool: True if table creation is successful, False otherwise
        """
        try:
            
            # Create the market data table
            query = """
            CREATE TABLE IF NOT EXISTS commodity_market_data (
                timestamp DateTime,
                exchange String,
                symbol_token String,
                interval String,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Int64,
                
                -- Partition by month for efficient querying of time ranges
                -- Useful for financial analysis which often happens by month/quarter
                _partition_date Date DEFAULT toDate(timestamp)
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(_partition_date)
            ORDER BY (timestamp, exchange, symbol_token, interval)
            SETTINGS index_granularity = 8192
            """
            
            self.client.execute(query)
            logger.info("Table 'commodity_market_data' created or already exists")
            
            # Add summary table for daily data (pre-aggregated)
            agg_query = """
            CREATE TABLE IF NOT EXISTS commodity_daily_summary (
                date Date,
                exchange String,
                symbol_token String,
                open Float64,
                high Float64,
                low Float64,
                close Float64,
                volume Int64,
                
                -- For efficient querying
                year UInt16 DEFAULT toYear(date),
                month UInt8 DEFAULT toMonth(date)
            ) ENGINE = MergeTree()
            PARTITION BY (exchange, toYYYYMM(date))
            ORDER BY (date, exchange, symbol_token)
            SETTINGS index_granularity = 8192
            """
            
            self.client.execute(agg_query)
            logger.info("Table 'commodity_daily_summary' created or already exists")
            
            return True
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            return False
            
    def insert_market_data(self, df: pd.DataFrame) -> bool:
        """
        Insert market data into ClickHouse
        
        Args:
            df: DataFrame containing market data
            
        Returns:
            bool: True if insertion is successful, False otherwise
        """
        if df.empty:
            logger.warning("Empty DataFrame provided, skipping insertion")
            return False
            
        try:
            # Convert DataFrame to a list of tuples for simple insertion
            records = []
            for _, row in df.iterrows():
                records.append((
                    row['timestamp'], 
                    row['exchange'],
                    row['symbol_token'],
                    row['interval'],
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    int(row['volume'])
                ))
            
            # Log data to diagnose issues
            logger.info(f"Sample record for insertion: {records[0] if records else 'No records'}")
            
            # Insert using simple method that doesn't rely on NumPy
            query = """
            INSERT INTO commodity_market_data (
                timestamp, exchange, symbol_token, interval, 
                open, high, low, close, volume
            ) VALUES
            """
            
            self.client.execute(query, records, types_check=True)
            logger.info(f"Successfully inserted {len(records)} rows into commodity_market_data")
            
            # If the data is daily, also insert into the summary table
            if 'ONE_DAY' in df['interval'].values:
                daily_df = df[df['interval'] == 'ONE_DAY'].copy()
                daily_df['date'] = daily_df['timestamp'].dt.date
                
                # Create daily records
                daily_records = []
                for _, row in daily_df.iterrows():
                    daily_records.append((
                        row['date'],
                        row['exchange'],
                        row['symbol_token'],
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row['volume'])
                    ))
                
                # Insert daily data
                if daily_records:
                    query = """
                    INSERT INTO commodity_daily_summary (
                        date, exchange, symbol_token, open, high, low, close, volume
                    ) VALUES
                    """
                    
                    self.client.execute(query, daily_records, types_check=True)
                    logger.info(f"Successfully inserted {len(daily_records)} rows into commodity_daily_summary")
            
            return True
        except Exception as e:
            logger.error(f"Failed to insert data: {str(e)}")
            return False
    
    def check_data_exists(self, exchange: str, symbol_token: str, interval: str, 
                          start_date: str, end_date: str) -> bool:
        """
        Check if data already exists for a given symbol and date range
        
        Args:
            exchange: Exchange name
            symbol_token: Symbol token
            interval: Data interval
            start_date: Start date string in 'YYYY-MM-DD' format
            end_date: End date string in 'YYYY-MM-DD' format
            
        Returns:
            bool: True if data exists, False otherwise
        """
        try:
            # First check if the table exists
            table_exists_query = f"""
            SELECT count() 
            FROM system.tables 
            WHERE database = '{CLICKHOUSE_DATABASE}' AND name = 'commodity_market_data'
            """
            
            result = self.client.execute(table_exists_query)
            if result[0][0] == 0:
                logger.warning("Table 'commodity_market_data' does not exist")
                return False
                
            # Now check for data
            query = f"""
            SELECT COUNT(*) 
            FROM commodity_market_data 
            WHERE exchange = '{exchange}' 
              AND symbol_token = '{symbol_token}' 
              AND interval = '{interval}'
              AND timestamp >= toDateTime('{start_date}')
              AND timestamp <= toDateTime('{end_date}')
            """
            
            result = self.client.execute(query)
            count = result[0][0]
            
            return count > 0
        except Exception as e:
            logger.error(f"Failed to check data existence: {str(e)}")
            return False
    
    def get_data_coverage(self, exchange: str, symbol_token: str, interval: str) -> Dict:
        """
        Get information about data coverage for a specific symbol
        
        Args:
            exchange: Exchange name
            symbol_token: Symbol token
            interval: Data interval
            
        Returns:
            Dict: Information about data coverage including:
                - first_date: First date with data
                - last_date: Last date with data
                - total_records: Total number of records
                - missing_dates: List of date ranges with missing data (if any)
        """
        try:
            # Get first and last dates
            range_query = f"""
            SELECT 
                min(timestamp) as first_date,
                max(timestamp) as last_date,
                count(*) as total_records
            FROM commodity_market_data 
            WHERE exchange = '{exchange}' 
              AND symbol_token = '{symbol_token}' 
              AND interval = '{interval}'
            """
            
            result = self.client.execute(range_query)
            
            if not result or not result[0][0]:
                return {
                    "first_date": None,
                    "last_date": None,
                    "total_records": 0,
                    "missing_dates": []
                }
                
            first_date, last_date, total_records = result[0]
            
            # For intervals less than a day, finding missing dates is complex and resource-intensive
            # For daily data, we can check for missing dates
            missing_dates = []
            if interval == 'ONE_DAY':
                # Find missing dates
                missing_query = f"""
                WITH 
                    toDate('{first_date}') as start_date,
                    toDate('{last_date}') as end_date,
                    arrayJoin(arrayMap(x -> toDate(start_date + x), range(toUInt64(end_date - start_date + 1)))) as all_dates
                SELECT 
                    all_dates as missing_date
                FROM commodity_market_data
                WHERE all_dates NOT IN (
                    SELECT DISTINCT toDate(timestamp)
                    FROM commodity_market_data
                    WHERE exchange = '{exchange}' 
                      AND symbol_token = '{symbol_token}' 
                      AND interval = '{interval}'
                      AND timestamp BETWEEN '{first_date}' AND '{last_date}'
                )
                ORDER BY missing_date
                """
                
                missing_result = self.client.execute(missing_query)
                missing_dates = [str(date[0]) for date in missing_result]
            
            return {
                "first_date": first_date,
                "last_date": last_date,
                "total_records": total_records,
                "missing_dates": missing_dates
            }
            
        except Exception as e:
            logger.error(f"Failed to get data coverage: {str(e)}")
            return {
                "error": str(e),
                "first_date": None,
                "last_date": None,
                "total_records": 0,
                "missing_dates": []
            }
