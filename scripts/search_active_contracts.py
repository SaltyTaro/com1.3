#!/usr/bin/env python
"""
Script to search for active commodity contracts and test them
"""
import os
import sys
import time
import logging
import pyotp
import json
from datetime import datetime, timedelta

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from SmartApi import SmartConnect
from config.settings import API_KEY, CLIENT_CODE, PASSWORD, TOTP_KEY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# List of commodity search terms
COMMODITY_SEARCH_TERMS = [
    "GOLD", "SILVER", "CRUDEOIL", "COPPER", "ZINC", 
    "LEAD", "NATURALGAS", "ALUMINIUM", "NICKEL"
]

def search_active_contracts():
    """Search for active commodity contracts and test them"""
    logger.info("Searching for active commodity contracts...")
    
    smart_api = SmartConnect(API_KEY)
    
    try:
        # Authenticate
        totp = pyotp.TOTP(TOTP_KEY).now()
        data = smart_api.generateSession(CLIENT_CODE, PASSWORD, totp)
        
        if not data['status']:
            logger.error(f"Authentication failed: {data['message']}")
            return False
            
        logger.info("Authentication successful")
        
        # Search for each commodity
        active_contracts = []
        
        for commodity in COMMODITY_SEARCH_TERMS:
            logger.info(f"Searching for {commodity} contracts...")
            try:
                # Try both MCX and MCX_FO exchanges
                for exchange in ["MCX", "MCX_FO"]:
                    search_result = smart_api.searchScrip(exchange, commodity)
                    
                    if search_result and search_result.get('status'):
                        contracts = search_result.get('data', [])
                        logger.info(f"Found {len(contracts)} contracts for {commodity} on {exchange}")
                        
                        if contracts:
                            # Log contract details
                            for contract in contracts:
                                contract_info = {
                                    "name": contract.get('tradingsymbol', ''),
                                    "exchange": exchange,
                                    "token": contract.get('symboltoken', ''),
                                    "expiry": contract.get('expiry', ''),
                                    "strike": contract.get('strike', ''),
                                    "lotsize": contract.get('lotsize', ''),
                                    "instrument_type": contract.get('instrumenttype', '')
                                }
                                
                                logger.info(f"Contract: {json.dumps(contract_info, indent=2)}")
                                active_contracts.append(contract_info)
                    else:
                        logger.info(f"No contracts found for {commodity} on {exchange}")
            except Exception as e:
                logger.error(f"Error searching for {commodity}: {str(e)}")
            
            # Add a small delay between searches
            time.sleep(1)
        
        # Save active contracts to file
        with open('active_contracts.json', 'w') as f:
            json.dump(active_contracts, f, indent=2)
        
        logger.info(f"Found a total of {len(active_contracts)} active contracts")
        
        # Test a few of the active contracts
        if active_contracts:
            logger.info("Testing some active contracts...")
            
            # Get last 7 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            date_format = "%Y-%m-%d %H:%M"
            from_date = start_date.strftime(date_format)
            to_date = end_date.strftime(date_format)
            
            # Test at most 5 contracts
            for i, contract in enumerate(active_contracts[:5]):
                logger.info(f"Testing contract: {contract['name']} ({contract['exchange']}:{contract['token']})")
                
                try:
                    params = {
                        "exchange": contract['exchange'],
                        "symboltoken": contract['token'],
                        "interval": "ONE_DAY",
                        "fromdate": from_date,
                        "todate": to_date
                    }
                    
                    logger.info(f"Parameters: {params}")
                    response = smart_api.getCandleData(params)
                    
                    if response and isinstance(response, dict):
                        logger.info(f"Response status: {response.get('status', 'No status')}")
                        logger.info(f"Response message: {response.get('message', 'No message')}")
                        
                        if response.get('status'):
                            data_points = len(response.get('data', []))
                            logger.info(f"Data points: {data_points}")
                            
                            if data_points > 0:
                                logger.info(f"First data point: {response.get('data', [])[0]}")
                                logger.info(f"Last data point: {response.get('data', [])[-1]}")
                                logger.info("✓ SUCCESS! Found data for this contract.")
                                
                                # Save successful contract details to separate file
                                with open('working_contract.json', 'w') as f:
                                    json.dump({
                                        "contract": contract,
                                        "params": params,
                                        "sample_data": response.get('data', [])[:2]
                                    }, f, indent=2)
                                
                                break
                            else:
                                logger.info("✗ No data points for this contract")
                        else:
                            logger.info(f"✗ Error: {response.get('message', 'Unknown error')}")
                    else:
                        logger.error("✗ Invalid response format")
                except Exception as e:
                    logger.error(f"✗ Error testing contract: {str(e)}")
                
                # Add a small delay between tests
                time.sleep(2)
        
        # Try searching for today's trading symbols specifically
        logger.info("Searching for today's trading symbols...")
        for commodity in ["GOLD", "CRUDEOIL"]:
            # Add today's month and year to the search term (e.g., GOLDMAY24)
            today = datetime.now()
            month_abbr = today.strftime("%b").upper()
            year_abbr = today.strftime("%y")
            
            search_term = f"{commodity}{month_abbr}{year_abbr}"
            logger.info(f"Searching for specific contract: {search_term}")
            
            try:
                search_result = smart_api.searchScrip("MCX", search_term)
                
                if search_result and search_result.get('status'):
                    contracts = search_result.get('data', [])
                    logger.info(f"Found {len(contracts)} specific contracts for {search_term}")
                    
                    if contracts:
                        # Test the first contract
                        contract = contracts[0]
                        contract_info = {
                            "name": contract.get('tradingsymbol', ''),
                            "exchange": "MCX",
                            "token": contract.get('symboltoken', '')
                        }
                        
                        logger.info(f"Testing specific contract: {contract_info['name']} ({contract_info['exchange']}:{contract_info['token']})")
                        
                        params = {
                            "exchange": contract_info['exchange'],
                            "symboltoken": contract_info['token'],
                            "interval": "ONE_DAY",
                            "fromdate": from_date,
                            "todate": to_date
                        }
                        
                        response = smart_api.getCandleData(params)
                        
                        if response and response.get('status'):
                            data_points = len(response.get('data', []))
                            logger.info(f"Data points for specific contract: {data_points}")
                            
                            if data_points > 0:
                                logger.info("✓ SUCCESS! Found data for this specific contract.")
                                
                                # Save successful contract details
                                with open(f'working_specific_contract_{search_term}.json', 'w') as f:
                                    json.dump({
                                        "contract": contract_info,
                                        "params": params,
                                        "sample_data": response.get('data', [])[:2]
                                    }, f, indent=2)
                else:
                    logger.info(f"No specific contracts found for {search_term}")
            except Exception as e:
                logger.error(f"Error searching for specific contract {search_term}: {str(e)}")
        
        # Logout
        smart_api.terminateSession(CLIENT_CODE)
        logger.info("Session terminated")
        
        return True
    except Exception as e:
        logger.error(f"Error during contract search: {str(e)}")
        return False

if __name__ == "__main__":
    if search_active_contracts():
        print("Contract search completed!")
    else:
        print("Contract search failed. Check logs for details.")
        sys.exit(1)