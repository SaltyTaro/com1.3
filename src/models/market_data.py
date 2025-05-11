"""
Data models for handling market data
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any, Optional

@dataclass
class CandleData:
    """Representation of a single candle/bar of market data"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    exchange: str
    symbol_token: str
    interval: str

    @classmethod
    def from_api_response(cls, data: Dict[str, Any], exchange: str, symbol_token: str, interval: str) -> 'CandleData':
        """
        Create a CandleData instance from API response data
        
        Args:
            data: Single candle data from API
            exchange: Exchange name
            symbol_token: Symbol token
            interval: Data interval
            
        Returns:
            CandleData: Structured candle data
        """
        return cls(
            timestamp=datetime.fromisoformat(data[0].replace('Z', '+00:00')),
            open=float(data[1]),
            high=float(data[2]),
            low=float(data[3]),
            close=float(data[4]),
            volume=int(data[5]),
            exchange=exchange,
            symbol_token=symbol_token,
            interval=interval
        )

@dataclass
class CommodityMetadata:
    """Metadata about a commodity instrument"""
    name: str
    exchange: str
    symbol_token: str
    instrument_type: str = "COMMODITY"
    expiry_date: Optional[datetime] = None
    
    @property
    def full_symbol(self) -> str:
        """
        Get the full symbol representation
        
        Returns:
            str: Full symbol name with exchange
        """
        return f"{self.exchange}:{self.name}"

@dataclass
class DataCoverage:
    """Information about data coverage for a symbol"""
    exchange: str
    symbol_token: str
    interval: str
    first_date: Optional[datetime]
    last_date: Optional[datetime]
    total_records: int
    missing_dates: List[str]
    
    @property
    def has_data(self) -> bool:
        """
        Check if there is any data available
        
        Returns:
            bool: True if data is available, False otherwise
        """
        return self.total_records > 0
    
    @property
    def coverage_summary(self) -> str:
        """
        Get a human-readable summary of data coverage
        
        Returns:
            str: Summary of data coverage
        """
        if not self.has_data:
            return "No data available"
            
        missing_count = len(self.missing_dates)
        if missing_count > 0:
            missing_str = f" with {missing_count} missing dates"
        else:
            missing_str = " with complete coverage"
            
        return (
            f"Data from {self.first_date.strftime('%Y-%m-%d')} to "
            f"{self.last_date.strftime('%Y-%m-%d')}{missing_str}"
        )