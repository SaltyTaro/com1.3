"""
Helper functions for the market data fetcher
"""
import os
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Generator, Dict, Any
import pandas as pd

from config.settings import DATE_FORMAT, LOG_FORMAT, LOG_LEVEL, LOG_FILE, IST

def setup_logging() -> None:
    """
    Set up logging configuration
    """
    # Create logs directory if it doesn't exist
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    
    # Configure logger
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL),
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

def date_range_chunks(start_date: datetime, end_date: datetime, days_per_chunk: int) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    Split a date range into chunks of a specific number of days
    
    Args:
        start_date: Start date
        end_date: End date
        days_per_chunk: Maximum number of days per chunk
        
    Yields:
        Tuple[datetime, datetime]: Start and end dates for each chunk
    """
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=days_per_chunk), end_date)
        yield current_start, current_end
        current_start = current_end + timedelta(minutes=1)

def format_time_elapsed(seconds: float) -> str:
    """
    Format time elapsed in a human-readable way
    
    Args:
        seconds: Time elapsed in seconds
        
    Returns:
        str: Formatted time string
    """
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"

def get_trading_dates(start_date: datetime, end_date: datetime) -> List[datetime]:
    """
    Get a list of trading dates (excluding weekends)
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        List[datetime]: List of trading dates
    """
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        # Skip weekends (5 = Saturday, 6 = Sunday)
        if current_date.weekday() < 5:
            dates.append(current_date)
        
        current_date += timedelta(days=1)
    
    return dates

def check_exchange_open(dt: datetime) -> bool:
    """
    Check if the exchange is open at the given datetime
    
    Args:
        dt: Datetime to check (assumed to be in IST)
        
    Returns:
        bool: True if exchange is open, False otherwise
    """
    # Check if it's a weekday (0-4 are Monday to Friday)
    if dt.weekday() >= 5:
        return False
    
    # Check if it's within trading hours (9:00 AM to 11:30 PM IST for MCX)
    # This is a simplification - actual trading hours vary by commodity type
    hour = dt.hour
    minute = dt.minute
    
    # Convert to minutes since midnight for easier comparison
    time_in_minutes = hour * 60 + minute
    
    # MCX trading hours: 9:00 AM to 11:30 PM IST
    trading_start = 9 * 60  # 9:00 AM
    trading_end = 23 * 60 + 30  # 11:30 PM
    
    return trading_start <= time_in_minutes <= trading_end

def validate_date_range(start_date: datetime, end_date: datetime) -> Tuple[bool, str]:
    """
    Validate a date range for historical data
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message)
    """
    # Check if end_date is not in the future
    now = datetime.now(IST)
    if end_date > now:
        return False, f"End date ({end_date}) cannot be in the future"
    
    # Check if start_date is before end_date
    if start_date >= end_date:
        return False, f"Start date ({start_date}) must be before end date ({end_date})"
    
    # Check if the range is not too long (more than 10 years)
    delta = end_date - start_date
    if delta.days > 365 * 10:
        return False, f"Date range too long: {delta.days} days (max is 3650 days/10 years)"
    
    return True, ""

def get_available_commodities() -> List[Dict[str, str]]:
    """
    Get a list of available commodities from settings
    
    Returns:
        List[Dict[str, str]]: List of commodity info dictionaries
    """
    from config.settings import COMMODITY_LIST
    
    return [
        {
            "name": name,
            "exchange": exchange,
            "symbol_token": symbol_token
        }
        for name, exchange, symbol_token in COMMODITY_LIST
    ]

def get_missing_dates(data: pd.DataFrame, start_date: datetime, end_date: datetime, frequency: str = 'D') -> List[datetime]:
    """
    Identify missing dates in a time series
    
    Args:
        data: DataFrame with a 'timestamp' column
        start_date: Expected start date
        end_date: Expected end date
        frequency: Frequency string ('D' for daily, 'H' for hourly, etc.)
        
    Returns:
        List[datetime]: List of missing dates
    """
    if data.empty:
        # If the data is empty, all dates are missing
        expected_dates = pd.date_range(start=start_date, end=end_date, freq=frequency)
        return expected_dates.tolist()
    
    # Create a Series of all the timestamps in the data
    actual_dates = pd.Series(data['timestamp'].unique())
    
    # Create a Series of all expected dates in the range
    expected_dates = pd.date_range(start=start_date, end=end_date, freq=frequency)
    
    # Find missing dates
    missing_dates = expected_dates[~expected_dates.isin(actual_dates)].tolist()
    
    return missing_dates