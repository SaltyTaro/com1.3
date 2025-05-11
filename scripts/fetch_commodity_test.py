#!/usr/bin/env python
"""
Script to test fetching commodity data with different symbol tokens and date formats
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

# Test various symbols and formats
COMMODITY_TESTS = [
    {
        "name": "GOLD",
        "exchange": "MCX_FO",
        "symbol_token": "234230",
        "date_format": "%Y-%m-%d %H:%M"
    },
    {
        "name": "GOLD",
        "exchange": "MCX",  # Try without the _FO suffix
        "symbol_token": "234230",
        "date_format": "%Y-%m-%d %H:%M"
    },
    {
        "name": "CRUDEOIL",
        "exchange": "MCX_FO",
        "symbol_token": "234219",
        "date_format": "%Y-%m-%d %H:%M"
    },
    {
        "name": "SILVER",
        "exchange": "MCX_FO",
        "symbol_token": "234235",
        "date_format": "%Y-%m-%d %H:%M"
    },
    # Try the most recent active contract for Gold (example)
    {
        "name": "GOLD30MAY24",  # Using a specific contract
        "exchange": "MCX_FO",
        "symbol_token": "243287",  # This is a made-up token for illustration
        "date_format": "%Y-%m-%d %H:%M"
    }
]

def test_commodity_fetching():
    """Test fetching historical data for different commodities"""
    logger.info("Testing commodity data fetching with different configurations...")
    
    smart_api = SmartConnect(API_KEY)
    
    try:
        # Authenticate
        totp = pyotp.TOTP(TOTP_KEY).now()
        data = smart_api.generateSession(CLIENT_CODE, PASSWORD, totp)
        
        if not data['status']:
            logger.error(f"Authentication failed: {data['message']}")
            return False
            
        logger.info("Authentication successful")
        
        # Calculate date range (last 7 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        # Try to find active contracts
        try:
            logger.info("Searching for active GOLD contracts...")
            search_result = smart_api.searchScrip("MCX_FO", "GOLD")
            if search_result and search_result.get('status'):
                logger.info(f"Search results: {search_result}")
                
                # Add found contracts to our test list
                for item in search_result.get('data', []):
                    if 'tradingsymbol' in item and 'symboltoken' in item:
                        COMMODITY_TESTS.append({
                            "name": item['tradingsymbol'],
                            "exchange": "MCX_FO",
                            "symbol_token": item['symboltoken'],
                            "date_format": "%Y-%m-%d %H:%M"
                        })
                        logger.info(f"Added {item['tradingsymbol']} to test list with token {item['symboltoken']}")
        except Exception as e:
            logger.error(f"Error searching for contracts: {str(e)}")
        
        # Test each commodity configuration
        for i, test_config in enumerate(COMMODITY_TESTS):
            logger.info(f"Test {i+1}/{len(COMMODITY_TESTS)}: {test_config['name']} ({test_config['exchange']}:{test_config['symbol_token']})")
            
            # Try with a small date range
            try:
                from_date = start_date.strftime(test_config['date_format'])
                to_date = end_date.strftime(test_config['date_format'])
                
                logger.info(f"Requesting data from {from_date} to {to_date}")
                
                params = {
                    "exchange": test_config['exchange'],
                    "symboltoken": test_config['symbol_token'],
                    "interval": "ONE_DAY",
                    "fromdate": from_date,
                    "todate": to_date
                }
                
                logger.info(f"Parameters: {params}")
                response = smart_api.getCandleData(params)
                
                if response:
                    logger.info(f"Response status: {response.get('status', 'No status')}")
                    logger.info(f"Response message: {response.get('message', 'No message')}")
                    
                    if response.get('status'):
                        logger.info(f"Data points: {len(response.get('data', []))}")
                        logger.info(f"First data point: {response.get('data', [])[0] if response.get('data') else 'No data'}")
                        logger.info("✓ SUCCESS! This configuration works.")
                    else:
                        logger.info("✗ This configuration failed with error response")
                else:
                    logger.error("✗ Empty response received")
            except Exception as e:
                logger.error(f"✗ Error in request: {str(e)}")
            
            # Add a small delay between requests to avoid rate limiting
            time.sleep(2)
        
        # Logout
        smart_api.terminateSession(CLIENT_CODE)
        logger.info("Session terminated")
        
        return True
    except Exception as e:
        logger.error(f"Error during testing: {str(e)}")
        return False

if __name__ == "__main__":
    if test_commodity_fetching():
        print("Commodity testing completed!")
    else:
        print("Commodity testing failed. Check logs for details.")
        sys.exit(1)