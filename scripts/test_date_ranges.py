#!/usr/bin/env python
"""
Script to test fetching commodity data with different date ranges
"""
import os
import sys
import time
import logging
import pyotp
from datetime import datetime, timedelta

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from SmartApi import SmartConnect
from config.settings import API_KEY, CLIENT_CODE, PASSWORD, TOTP_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Test date ranges (from oldest to newest)
DATE_RANGES = [
    # Try historical ranges
    {
        "name": "2020 Q1",
        "from_date": "2020-01-01 09:00",
        "to_date": "2020-03-31 16:00"
    },
    {
        "name": "2021 Q1",
        "from_date": "2021-01-01 09:00",
        "to_date": "2021-03-31 16:00"
    },
    {
        "name": "2022 Q1",
        "from_date": "2022-01-01 09:00",
        "to_date": "2022-03-31 16:00"
    },
    {
        "name": "2023 Q1",
        "from_date": "2023-01-01 09:00",
        "to_date": "2023-03-31 16:00"
    },
    {
        "name": "2024 Q1",
        "from_date": "2024-01-01 09:00",
        "to_date": "2024-03-31 16:00"
    },
    # Try more recent ranges
    {
        "name": "Last 3 months",
        "from_date": (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d 09:00"),
        "to_date": datetime.now().strftime("%Y-%m-%d 16:00")
    },
    {
        "name": "Last month",
        "from_date": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d 09:00"),
        "to_date": datetime.now().strftime("%Y-%m-%d 16:00")
    },
    {
        "name": "Last week",
        "from_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d 09:00"),
        "to_date": datetime.now().strftime("%Y-%m-%d 16:00")
    }
]

def test_date_ranges():
    """Test fetching historical data for different date ranges"""
    logger.info("Testing commodity data fetching with different date ranges...")
    
    smart_api = SmartConnect(API_KEY)
    
    try:
        # Authenticate
        totp = pyotp.TOTP(TOTP_KEY).now()
        data = smart_api.generateSession(CLIENT_CODE, PASSWORD, totp)
        
        if not data['status']:
            logger.error(f"Authentication failed: {data['message']}")
            return False
            
        logger.info("Authentication successful")
        
        # We'll use GOLD as our test commodity since we know the format works
        exchange = "MCX"
        symbol_token = "234230"
        
        # Test each date range
        for i, date_range in enumerate(DATE_RANGES):
            logger.info(f"Test {i+1}/{len(DATE_RANGES)}: {date_range['name']} ({date_range['from_date']} to {date_range['to_date']})")
            
            try:
                params = {
                    "exchange": exchange,
                    "symboltoken": symbol_token,
                    "interval": "ONE_DAY",
                    "fromdate": date_range['from_date'],
                    "todate": date_range['to_date']
                }
                
                logger.info(f"Parameters: {params}")
                response = smart_api.getCandleData(params)
                
                if response:
                    logger.info(f"Response status: {response.get('status', 'No status')}")
                    logger.info(f"Response message: {response.get('message', 'No message')}")
                    
                    if response.get('status'):
                        data_points = len(response.get('data', []))
                        logger.info(f"Data points: {data_points}")
                        
                        if data_points > 0:
                            logger.info(f"First data point: {response.get('data', [])[0]}")
                            logger.info(f"Last data point: {response.get('data', [])[-1]}")
                            logger.info("✓ SUCCESS! Found data for this range.")
                        else:
                            logger.info("✗ No data points in this range")
                    else:
                        logger.info(f"✗ Error: {response.get('message', 'Unknown error')}")
                else:
                    logger.error("✗ Empty response received")
            except Exception as e:
                logger.error(f"✗ Error in request: {str(e)}")
            
            # Add a small delay between requests to avoid rate limiting
            time.sleep(2)
        
        # Try a few other commodities with the most successful date range
        if len(DATE_RANGES) > 0:
            # Find the date range with the most data points
            best_range = None
            max_points = 0
            
            for date_range in DATE_RANGES:
                try:
                    params = {
                        "exchange": exchange,
                        "symboltoken": symbol_token,
                        "interval": "ONE_DAY",
                        "fromdate": date_range['from_date'],
                        "todate": date_range['to_date']
                    }
                    
                    response = smart_api.getCandleData(params)
                    
                    if response and response.get('status'):
                        data_points = len(response.get('data', []))
                        if data_points > max_points:
                            max_points = data_points
                            best_range = date_range
                except Exception:
                    continue
            
            if best_range:
                logger.info(f"Testing other commodities with best date range: {best_range['name']}")
                
                other_commodities = [
                    {"name": "SILVER", "token": "234235"},
                    {"name": "CRUDEOIL", "token": "234219"},
                    {"name": "COPPER", "token": "234226"}
                ]
                
                for commodity in other_commodities:
                    try:
                        logger.info(f"Testing {commodity['name']} ({commodity['token']})...")
                        
                        params = {
                            "exchange": exchange,
                            "symboltoken": commodity['token'],
                            "interval": "ONE_DAY",
                            "fromdate": best_range['from_date'],
                            "todate": best_range['to_date']
                        }
                        
                        response = smart_api.getCandleData(params)
                        
                        if response and response.get('status'):
                            data_points = len(response.get('data', []))
                            logger.info(f"Data points for {commodity['name']}: {data_points}")
                            
                            if data_points > 0:
                                logger.info("✓ SUCCESS! Found data for this commodity.")
                            else:
                                logger.info("✗ No data found for this commodity")
                        else:
                            logger.info(f"✗ Error response for {commodity['name']}")
                    except Exception as e:
                        logger.error(f"✗ Error testing {commodity['name']}: {str(e)}")
                    
                    time.sleep(2)
        
        # Logout
        smart_api.terminateSession(CLIENT_CODE)
        logger.info("Session terminated")
        
        return True
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}")
        return False

if __name__ == "__main__":
    if test_date_ranges():
        print("Date range testing completed!")
    else:
        print("Date range testing failed. Check logs for details.")
        sys.exit(1)