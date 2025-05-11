#!/usr/bin/env python
"""
Script to test basic Angel Broking SmartAPI functionality
"""
import os
import sys
import logging
import pyotp
from datetime import datetime, timedelta

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from SmartApi import SmartConnect
from config.settings import API_KEY, CLIENT_CODE, PASSWORD, TOTP_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_authentication():
    """Test authentication with SmartAPI"""
    logger.info("Testing SmartAPI authentication...")
    
    smart_api = SmartConnect(API_KEY)
    
    try:
        totp = pyotp.TOTP(TOTP_KEY).now()
        logger.info(f"Generated TOTP: {totp}")
        
        data = smart_api.generateSession(CLIENT_CODE, PASSWORD, totp)
        
        if data['status']:
            logger.info("Authentication successful!")
            logger.info(f"Status: {data['status']}")
            logger.info(f"Message: {data['message']}")
            
            # Display token information
            logger.info(f"JWT Token: {data['data']['jwtToken'][:20]}...")
            logger.info(f"Refresh Token: {data['data']['refreshToken'][:20]}...")
            
            # Get user profile
            logger.info("Fetching user profile...")
            user_profile = smart_api.getProfile(data['data']['refreshToken'])
            logger.info(f"User profile status: {user_profile['status']}")
            
            if user_profile['status']:
                logger.info(f"User: {user_profile['data']['name']}")
                logger.info(f"Email: {user_profile['data']['email']}")
                logger.info(f"Exchanges enabled: {user_profile['data']['exchanges']}")
            
            # Test a basic API call to get feed token
            feed_token = smart_api.getfeedToken()
            logger.info(f"Feed token: {feed_token[:20]}..." if feed_token else "Failed to get feed token")
            
            # Test a basic market data call (LTP)
            try:
                logger.info("Testing LTP data API...")
                ltp_data = smart_api.ltpData("NSE", "SBIN-EQ", "3045")
                logger.info(f"LTP data status: {ltp_data['status']}")
                logger.info(f"LTP data: {ltp_data}")
            except Exception as e:
                logger.error(f"Error in LTP data API: {str(e)}")
            
            # Test historical data API
            try:
                # Try with a very recent date for a common stock
                end_date = datetime.now()
                start_date = end_date - timedelta(days=5)
                
                logger.info("Testing historical data API...")
                logger.info(f"Requesting data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                
                params = {
                    "exchange": "NSE",
                    "symboltoken": "3045",  # SBIN
                    "interval": "ONE_DAY",
                    "fromdate": start_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "todate": end_date.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                logger.info(f"Historical data params: {params}")
                hist_data = smart_api.getCandleData(params)
                
                logger.info(f"Historical data status: {hist_data['status'] if hist_data else 'No response'}")
                logger.info(f"Historical data: {hist_data}")
            except Exception as e:
                logger.error(f"Error in historical data API: {str(e)}")
            
            # Test commodity data
            try:
                # Try with a very recent date for gold
                end_date = datetime.now()
                start_date = end_date - timedelta(days=5)
                
                logger.info("Testing commodity historical data API...")
                logger.info(f"Requesting GOLD data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                
                params = {
                    "exchange": "MCX_FO",
                    "symboltoken": "234230",  # GOLD
                    "interval": "ONE_DAY",
                    "fromdate": start_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "todate": end_date.strftime("%Y-%m-%d %H:%M:%S")
                }
                
                logger.info(f"Commodity data params: {params}")
                commodity_data = smart_api.getCandleData(params)
                
                logger.info(f"Commodity data status: {commodity_data['status'] if commodity_data else 'No response'}")
                logger.info(f"Commodity data: {commodity_data}")
            except Exception as e:
                logger.error(f"Error in commodity data API: {str(e)}")
            
            # Logout
            logout = smart_api.terminateSession(CLIENT_CODE)
            logger.info(f"Logout status: {logout['status']}")
            
            return True
        else:
            logger.error(f"Authentication failed: {data['message']}")
            return False
    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        return False

if __name__ == "__main__":
    if test_authentication():
        print("API test completed successfully!")
    else:
        print("API test failed. Check logs for details.")
        sys.exit(1)