#!/usr/bin/env python
"""
Script to create ClickHouse database and tables for commodity market data

Usage:
    python create_tables.py
"""
import os
import sys
import logging

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.clickhouse_client import ClickHouseClient
from src.utils.helpers import setup_logging

logger = logging.getLogger(__name__)

def main():
    """
    Create ClickHouse database and tables
    """
    # Set up logging
    setup_logging()
    
    logger.info("Setting up ClickHouse database and tables")
    
    # Initialize ClickHouse client
    db_client = ClickHouseClient()
    
    # Test basic connection to ClickHouse server
    if not db_client.test_connection():
        logger.error("Failed to connect to ClickHouse server. Please check settings.")
        return False
    
    # Create database first
    logger.info("Creating database if it doesn't exist")
    if not db_client.create_database():
        logger.error("Failed to create database")
        return False
        
    # Now the database exists and we can proceed with table creation
    
    # Create market data table
    if not db_client.create_market_data_table():
        logger.error("Failed to create market data table")
        return False
    
    logger.info("Database and tables created successfully")
    return True

if __name__ == "__main__":
    if main():
        print("ClickHouse database and tables set up successfully!")
    else:
        print("Failed to set up ClickHouse database and tables. Check logs for details.")
        sys.exit(1)