#!/usr/bin/env python
"""
Main script for fetching historical commodity data and storing it in ClickHouse

Usage:
    python fetch_data.py [options]

Options:
    --interval INTERVAL    Data interval (1min, 5min, 15min, 30min, 1hour, 1day) [default: 1day]
    --start-date DATE      Start date (YYYY-MM-DD) [default: 5 years ago]
    --end-date DATE        End date (YYYY-MM-DD) [default: today]
    --exchange EXCHANGE    Exchange name (MCX_FO, NCX_FO) [default: all]
    --commodity SYMBOL     Commodity name (e.g., GOLD, SILVER) [default: all]
    --setup-db             Set up ClickHouse database and tables only
    --force-fetch          Force fetching data even if it already exists in the database
    --list-commodities     List available commodities and exit
    --check-coverage       Check data coverage for commodities
    --help                 Show this help message and exit
"""
import os
import sys
import time
import argparse
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    START_DATE, END_DATE, DEFAULT_INTERVAL, INTERVALS, 
    COMMODITY_LIST, IST, DATE_FORMAT, EXCHANGE_MAP
)
from src.api.smart_api_client import SmartAPIClient
from src.db.clickhouse_client import ClickHouseClient
from src.utils.helpers import (
    setup_logging, date_range_chunks, format_time_elapsed, 
    validate_date_range, get_available_commodities
)

logger = logging.getLogger(__name__)

def parse_args():
    """
    Parse command line arguments
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Fetch historical commodity data and store it in ClickHouse"
    )
    
    parser.add_argument(
        "--interval", type=str, choices=list(INTERVALS.keys()), default="1day",
        help="Data interval (1min, 5min, 15min, 30min, 1hour, 1day) [default: 1day]"
    )
    
    parser.add_argument(
        "--start-date", type=str, 
        help="Start date (YYYY-MM-DD) [default: 5 years ago]"
    )
    
    parser.add_argument(
        "--end-date", type=str, 
        help="End date (YYYY-MM-DD) [default: today]"
    )
    
    parser.add_argument(
        "--exchange", type=str, 
        help="Exchange name (MCX_FO, NCX_FO) [default: all]"
    )
    
    parser.add_argument(
        "--commodity", type=str, 
        help="Commodity name (e.g., GOLD, SILVER) [default: all]"
    )
    
    parser.add_argument(
        "--setup-db", action="store_true",
        help="Set up ClickHouse database and tables only"
    )
    
    parser.add_argument(
        "--force-fetch", action="store_true",
        help="Force fetching data even if it already exists in the database"
    )
    
    parser.add_argument(
        "--list-commodities", action="store_true",
        help="List available commodities and exit"
    )
    
    parser.add_argument(
        "--check-coverage", action="store_true",
        help="Check data coverage for commodities"
    )
    
    return parser.parse_args()

def setup_database() -> bool:
    """
    Set up ClickHouse database and tables
    
    Returns:
        bool: True if setup is successful, False otherwise
    """
    logger.info("Setting up ClickHouse database and tables")
    
    db_client = ClickHouseClient()
    
    if not db_client.test_connection():
        logger.error("Failed to connect to ClickHouse server")
        return False
    
    if not db_client.create_database():
        logger.error("Failed to create database")
        return False
    
    if not db_client.create_market_data_table():
        logger.error("Failed to create market data table")
        return False
    
    logger.info("Database setup completed successfully")
    return True

def list_commodities() -> None:
    """
    List available commodities
    """
    commodities = get_available_commodities()
    
    print("\nAvailable Commodities:")
    print("-" * 60)
    print(f"{'Name':<15} {'Exchange':<10} {'Symbol Token':<15}")
    print("-" * 60)
    
    for commodity in commodities:
        print(f"{commodity['name']:<15} {commodity['exchange']:<10} {commodity['symbol_token']:<15}")
    
    print("\nUse these names with the --commodity option to fetch specific commodity data")

def check_data_coverage(db_client: ClickHouseClient, interval: str) -> None:
    """
    Check data coverage for all commodities
    
    Args:
        db_client: ClickHouse client
        interval: Data interval
    """
    commodities = get_available_commodities()
    
    print("\nData Coverage Report:")
    print("-" * 100)
    print(f"{'Name':<15} {'Exchange':<10} {'Symbol Token':<15} {'Coverage':<60}")
    print("-" * 100)
    
    for commodity in commodities:
        coverage = db_client.get_data_coverage(
            commodity['exchange'], 
            commodity['symbol_token'], 
            INTERVALS[interval]
        )
        
        if coverage['total_records'] == 0:
            coverage_str = "No data available"
        else:
            first_date = coverage['first_date'].strftime('%Y-%m-%d')
            last_date = coverage['last_date'].strftime('%Y-%m-%d')
            missing_count = len(coverage['missing_dates'])
            
            if missing_count > 0:
                coverage_str = f"{first_date} to {last_date} (with {missing_count} missing dates)"
            else:
                coverage_str = f"{first_date} to {last_date} (complete coverage)"
        
        print(f"{commodity['name']:<15} {commodity['exchange']:<10} {commodity['symbol_token']:<15} {coverage_str:<60}")

def fetch_and_store_data(
    api_client: SmartAPIClient, 
    db_client: ClickHouseClient,
    exchange: str,
    symbol_token: str,
    symbol_name: str,
    interval: str,
    start_date: datetime,
    end_date: datetime,
    force_fetch: bool = False
) -> Tuple[int, int]:
    """
    Fetch historical data for a commodity and store it in ClickHouse
    
    Args:
        api_client: SmartAPI client
        db_client: ClickHouse client
        exchange: Exchange name
        symbol_token: Symbol token
        symbol_name: Symbol name (for logging)
        interval: Data interval
        start_date: Start date
        end_date: End date
        force_fetch: Whether to force fetching data even if it already exists
        
    Returns:
        Tuple[int, int]: (number of chunks processed, total records fetched)
    """
    logger.info(f"Fetching data for {symbol_name} ({exchange}:{symbol_token}) from {start_date} to {end_date} with interval {interval}")
    
    # Check if data already exists in the database
    if not force_fetch and db_client.check_data_exists(
        exchange, symbol_token, interval, 
        start_date.strftime('%Y-%m-%d'), 
        end_date.strftime('%Y-%m-%d')
    ):
        logger.info(f"Data already exists for {symbol_name}. Use --force-fetch to fetch anyway.")
        return 0, 0
    
    # Fetch data in chunks
    chunks_processed = 0
    total_records = 0
    
    try:
        for chunk_df in api_client.get_chunked_historical_data(
            exchange, symbol_token, interval, start_date, end_date
        ):
            if chunk_df is not None and not chunk_df.empty:
                # Log the data received
                logger.info(f"Received data chunk with {len(chunk_df)} records for {symbol_name}")
                logger.info(f"Date range: {chunk_df['timestamp'].min()} to {chunk_df['timestamp'].max()}")
                
                # Insert data into ClickHouse
                if db_client.insert_market_data(chunk_df):
                    chunks_processed += 1
                    total_records += len(chunk_df)
                    logger.info(f"Successfully stored {len(chunk_df)} records for {symbol_name} "
                                f"({chunks_processed} chunks, {total_records} total records)")
                else:
                    logger.error(f"Failed to store data for {symbol_name}")
        
        if total_records == 0:
            logger.warning(f"No data found for {symbol_name} in the specified date range")
    except Exception as e:
        logger.error(f"Error fetching data for {symbol_name}: {str(e)}")
    
    return chunks_processed, total_records

def main():
    """
    Main function for fetching and storing historical commodity data
    """
    # Set up logging
    setup_logging()
    
    # Parse command line arguments
    args = parse_args()
    
    # List commodities if requested
    if args.list_commodities:
        list_commodities()
        return
    
    # Set up database if requested
    if args.setup_db:
        if setup_database():
            logger.info("Database setup completed successfully")
        else:
            logger.error("Database setup failed")
        return
    
    # Initialize ClickHouse client
    db_client = ClickHouseClient()
    
    # Test connection to ClickHouse
    if not db_client.test_connection():
        logger.error("Failed to connect to ClickHouse server. Please check settings.")
        return
        
    # Ensure database and tables exist
    logger.info("Ensuring database and tables exist...")
    if not db_client.create_database():
        logger.error("Failed to create database")
        return
        
    if not db_client.create_market_data_table():
        logger.error("Failed to create market data tables")
        return
    
    # Check data coverage if requested
    if args.check_coverage:
        interval = args.interval if args.interval else "1day"
        check_data_coverage(db_client, interval)
        return
    
    # Initialize SmartAPI client
    api_client = SmartAPIClient()
    
    # Authenticate with SmartAPI
    if not api_client.authenticate():
        logger.error("Failed to authenticate with SmartAPI. Please check credentials.")
        return
    

    # Get interval
    interval_key = args.interval if args.interval else "1day"
    interval = INTERVALS[interval_key]
    
    # Parse dates with a smaller default range for testing
    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(hour=9, minute=0, second=0)
        start_date = IST.localize(start_date) if start_date.tzinfo is None else start_date
    else:
        # Default to just last 7 days for testing
        start_date = END_DATE - timedelta(days=7)
    
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        end_date = IST.localize(end_date) if end_date.tzinfo is None else end_date
    else:
        end_date = END_DATE
    
    # Validate date range
    is_valid, error_message = validate_date_range(start_date, end_date)
    if not is_valid:
        logger.error(error_message)
        return
    
    # Get list of commodities to fetch
    commodities = get_available_commodities()
    
    # Filter by exchange if specified
    if args.exchange:
        commodities = [c for c in commodities if c['exchange'] == args.exchange]
    
    # Filter by commodity if specified
    if args.commodity:
        commodities = [c for c in commodities if c['name'] == args.commodity]
    
    if not commodities:
        logger.error("No commodities found matching the specified criteria")
        return
    
    # Display summary of what we're going to do
    logger.info(f"Fetching data for {len(commodities)} commodities from {start_date} to {end_date} with interval {interval_key}")
    
    # Fetch and store data for each commodity
    start_time = time.time()
    total_chunks = 0
    total_records = 0
    
    for commodity in commodities:
        logger.info(f"Processing {commodity['name']} ({commodity['exchange']}:{commodity['symbol_token']})")
        
        chunks, records = fetch_and_store_data(
            api_client, db_client,
            commodity['exchange'], commodity['symbol_token'], commodity['name'],
            interval, start_date, end_date, args.force_fetch
        )
        
        total_chunks += chunks
        total_records += records
    
    # Log out of SmartAPI
    api_client.logout()
    
    # Display summary
    elapsed_time = time.time() - start_time
    logger.info(f"Completed fetching data for {len(commodities)} commodities")
    logger.info(f"Total chunks processed: {total_chunks}")
    logger.info(f"Total records fetched: {total_records}")
    logger.info(f"Total time elapsed: {format_time_elapsed(elapsed_time)}")

if __name__ == "__main__":
    main()