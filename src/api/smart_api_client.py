"""
SmartAPI client wrapper for fetching historical commodity data
"""
import time
import logging
import pyotp
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Generator, Any, Optional
import pandas as pd

from SmartApi import SmartConnect
from config.settings import (
    API_KEY, CLIENT_CODE, PASSWORD, TOTP_KEY, MAX_RETRY_ATTEMPTS, 
    RETRY_DELAY, MAX_DAYS_PER_REQUEST, MAX_REQUEST_PER_MINUTE, 
    DATE_FORMAT, EXCHANGE_MAP
)

logger = logging.getLogger(__name__)

class SmartAPIClient:
    """
    Wrapper around SmartAPI for fetching historical commodity data
    """
    def __init__(self):
        self.api = SmartConnect(API_KEY)
        self.is_authenticated = False
        self.auth_token = None
        self.refresh_token = None
        self.feed_token = None
        self.request_count = 0
        self.last_request_time = None

    def authenticate(self) -> bool:
        """
        Authenticate with SmartAPI using credentials
        
        Returns:
            bool: True if authentication is successful, False otherwise
        """
        try:
            totp = pyotp.TOTP(TOTP_KEY).now()
            data = self.api.generateSession(CLIENT_CODE, PASSWORD, totp)
            
            if data['status']:
                self.auth_token = data['data']['jwtToken']
                self.refresh_token = data['data']['refreshToken']
                self.feed_token = self.api.getfeedToken()
                self.is_authenticated = True
                logger.info("Authentication successful")
                return True
            else:
                logger.error(f"Authentication failed: {data['message']}")
                return False
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False

    def _check_rate_limit(self):
        """
        Check and apply rate limiting to prevent API throttling
        """
        current_time = time.time()
        
        if self.last_request_time:
            # Reset counter if more than a minute has passed
            if current_time - self.last_request_time > 60:
                self.request_count = 0
                
            # If we've hit the limit, wait until the next minute
            elif self.request_count >= MAX_REQUEST_PER_MINUTE:
                wait_time = 60 - (current_time - self.last_request_time)
                if wait_time > 0:
                    logger.info(f"Rate limit reached. Waiting for {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                self.request_count = 0
        
        self.last_request_time = current_time
        self.request_count += 1

    def _get_candle_data_with_retry(self, params: Dict) -> Optional[Dict]:
        """
        Call getCandleData with retry logic
        
        Args:
            params: Parameters for getCandleData
            
        Returns:
            Optional[Dict]: API response or None if all retries fail
        """
        for attempt in range(MAX_RETRY_ATTEMPTS):
            try:
                self._check_rate_limit()
                logger.info(f"Making API request with params: {params}")
                
                # Add diagnostic try-except block to capture raw response
                try:
                    import requests
                    import json
                    
                    # Manually construct the API request to see the raw response
                    url = self.api._rootUrl + self.api._routes["api.candle.data"]
                    headers = self.api.requestHeaders()
                    headers["Authorization"] = f"Bearer {self.auth_token}"
                    
                    logger.info(f"Making direct API request to: {url}")
                    logger.info(f"Using headers: {headers}")
                    
                    raw_resp = requests.post(url, json=params, headers=headers)
                    logger.info(f"Raw API response status: {raw_resp.status_code}")
                    logger.info(f"Raw API response text: {raw_resp.text[:500]}")  # Log first 500 chars
                    
                    if raw_resp.status_code != 200:
                        logger.error(f"API returned non-200 status code: {raw_resp.status_code}")
                        time.sleep(RETRY_DELAY)
                        continue
                        
                except Exception as e:
                    logger.error(f"Error in direct API request: {str(e)}")
                
                # Continue with normal flow
                response = self.api.getCandleData(params)
                
                # Check if response is empty
                if response is None or response == '':
                    logger.error(f"Empty response received from API on attempt {attempt+1}")
                    time.sleep(RETRY_DELAY)
                    continue
                    
                if response and response.get('status'):
                    return response
                
                error_msg = response.get('message', 'Unknown error') if response else 'Empty response'
                logger.warning(f"API request failed: {error_msg}")
                
                # If token expired, try to refresh session
                if response and (response.get('message') == 'Token Expired' or not self.is_authenticated):
                    logger.info("Token expired, attempting to re-authenticate")
                    if self.authenticate():
                        continue
                
                # Wait before retrying
                time.sleep(RETRY_DELAY)
            except Exception as e:
                logger.error(f"Error in API request (attempt {attempt+1}/{MAX_RETRY_ATTEMPTS}): {str(e)}")
                time.sleep(RETRY_DELAY)
        
        logger.error(f"Failed to get data after {MAX_RETRY_ATTEMPTS} attempts")
        return None

    def get_historical_data(
        self, 
        exchange: str, 
        symbol_token: str, 
        interval: str, 
        from_date: datetime, 
        to_date: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Get historical data for a specific commodity for a date range
        
        Args:
            exchange: Exchange name (e.g., "MCX")
            symbol_token: Symbol token
            interval: Data interval (e.g., "ONE_DAY", "ONE_HOUR")
            from_date: Start date
            to_date: End date
            
        Returns:
            Optional[pd.DataFrame]: DataFrame with historical data or None if request fails
        """
        if not self.is_authenticated and not self.authenticate():
            return None
            
        params = {
            "exchange": exchange,
            "symboltoken": symbol_token,
            "interval": interval,
            "fromdate": from_date.strftime(DATE_FORMAT),
            "todate": to_date.strftime(DATE_FORMAT)
        }
        
        logger.info(f"Fetching data for {exchange}:{symbol_token} from {from_date} to {to_date}")
        response = self._get_candle_data_with_retry(params)
        
        if not response:
            logger.error(f"No response from API for {exchange}:{symbol_token}")
            return None
            
        if not response.get('status'):
            error_msg = response.get('message', 'Unknown error')
            logger.error(f"Failed to get data: {error_msg}")
            return None
        
        # Process the data
        data = response.get('data', [])
        if not data:
            logger.warning(f"No data returned for {exchange}:{symbol_token} in the specified period")
            return None
            
        # Convert to DataFrame
        logger.info(f"Received {len(data)} data points for {exchange}:{symbol_token}")
        
        # Log the first and last data point for debugging
        if data:
            logger.info(f"First data point: {data[0]}")
            if len(data) > 1:
                logger.info(f"Last data point: {data[-1]}")
        
        try:
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Add symbol information
            df['exchange'] = exchange
            df['symbol_token'] = symbol_token
            df['interval'] = interval
            
            # Convert price columns to float
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col].astype(float)
                
            # Convert volume to int
            df['volume'] = df['volume'].astype(int)
            
            logger.info(f"Successfully processed {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            return None

    def get_chunked_historical_data(
        self, 
        exchange: str, 
        symbol_token: str, 
        interval: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Get historical data in chunks to handle long date ranges
        
        Args:
            exchange: Exchange name
            symbol_token: Symbol token
            interval: Data interval
            start_date: Overall start date
            end_date: Overall end date
            
        Yields:
            pd.DataFrame: Chunks of historical data
        """
        current_start = start_date
        
        while current_start < end_date:
            # Calculate the end of the current chunk
            current_end = min(current_start + timedelta(days=MAX_DAYS_PER_REQUEST), end_date)
            
            # Get data for the current chunk
            chunk_df = self.get_historical_data(
                exchange, symbol_token, interval, current_start, current_end
            )
            
            if chunk_df is not None and not chunk_df.empty:
                yield chunk_df
            
            # Move to the next chunk
            current_start = current_end + timedelta(minutes=1)
            
            # Small delay between chunks to avoid hitting rate limits
            time.sleep(1)

    def logout(self) -> bool:
        """
        Terminate the API session
        
        Returns:
            bool: True if logout is successful, False otherwise
        """
        try:
            if self.is_authenticated:
                logout_response = self.api.terminateSession(CLIENT_CODE)
                if logout_response.get('status'):
                    logger.info("Logged out successfully")
                    self.is_authenticated = False
                    return True
                else:
                    logger.error(f"Logout failed: {logout_response.get('message', 'Unknown error')}")
            return False
        except Exception as e:
            logger.error(f"Logout error: {str(e)}")
            return False