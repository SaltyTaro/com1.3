#!/usr/bin/env python
"""
Script to test the database insertion functionality
"""
import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.clickhouse_client import ClickHouseClient
from src.utils.helpers import setup_logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_insertion():
    """Test inserting data into the ClickHouse database"""
    logger.info("Testing database insertion...")
    
    # Create a sample DataFrame with test data
    # This mimics the structure of the data we get from the API
    test_data = [
        {
            "timestamp": datetime.now() - timedelta(days=4),
            "exchange": "MCX",
            "symbol_token": "440939",
            "interval": "ONE_DAY",
            "open": 97421.0,
            "high": 97449.0,
            "low": 95944.0,
            "close": 96276.0,
            "volume": 34
        },
        {
            "timestamp": datetime.now() - timedelta(days=3),
            "exchange": "MCX",
            "symbol_token": "440939",
            "interval": "ONE_DAY",
            "open": 96276.0,
            "high": 96500.0,
            "low": 95800.0,
            "close": 96100.0,
            "volume": 42
        },
        {
            "timestamp": datetime.now() - timedelta(days=2),
            "exchange": "MCX",
            "symbol_token": "440939",
            "interval": "ONE_DAY",
            "open": 96100.0,
            "high": 96700.0,
            "low": 95900.0,
            "close": 96400.0,
            "volume": 56
        },
        {
            "timestamp": datetime.now() - timedelta(days=1),
            "exchange": "MCX",
            "symbol_token": "440939",
            "interval": "ONE_DAY",
            "open": 96400.0,
            "high": 96696.0,
            "low": 95179.0,
            "close": 96043.0,
            "volume": 85
        }
    ]
    
    df = pd.DataFrame(test_data)
    logger.info(f"Created test DataFrame with {len(df)} rows")
    
    # Initialize ClickHouse client
    db_client = ClickHouseClient()
    
    # Test connection
    if not db_client.test_connection():
        logger.error("Failed to connect to ClickHouse. Please check settings.")
        return False
    
    # Create database and tables if they don't exist
    logger.info("Creating database and tables if they don't exist...")
    if not db_client.create_database():
        logger.error("Failed to create database")
        return False
    
    if not db_client.create_market_data_table():
        logger.error("Failed to create market data tables")
        return False
    
    # Insert test data
    logger.info("Attempting to insert test data...")
    if db_client.insert_market_data(df):
        logger.info("Successfully inserted test data")
        
        # Check if the data was inserted correctly
        logger.info("Checking if data exists in the database...")
        result = db_client.check_data_exists(
            "MCX", 
            "440939", 
            "ONE_DAY", 
            (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            datetime.now().strftime('%Y-%m-%d')
        )
        
        if result:
            logger.info("Test data found in the database!")
        else:
            logger.warning("Could not find test data in the database")
        
        # Get data coverage info
        logger.info("Getting data coverage information...")
        coverage = db_client.get_data_coverage("MCX", "440939", "ONE_DAY")
        logger.info(f"Data coverage: {coverage}")
        
        return True
    else:
        logger.error("Failed to insert test data")
        return False

if __name__ == "__main__":
    setup_logging()
    if test_database_insertion():
        print("Database test completed successfully!")
    else:
        print("Database test failed. Check logs for details.")
        sys.exit(1)