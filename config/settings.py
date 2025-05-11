"""
Configuration settings for the Commodities Data Fetcher
"""
import os
import json
from datetime import datetime, timedelta
import pytz

# API Settings
API_KEY = os.environ.get("SMARTAPI_KEY", "")
CLIENT_CODE = os.environ.get("SMARTAPI_CLIENT_CODE", "")
PASSWORD = os.environ.get("SMARTAPI_PASSWORD", "")
TOTP_KEY = os.environ.get("SMARTAPI_TOTP_KEY", "")  # Your OTP secret key

# ClickHouse Settings
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "market_data")

# Data Fetching Settings
DATE_FORMAT = "%Y-%m-%d %H:%M"  # Changed back to format without seconds
MAX_RETRY_ATTEMPTS = 5
RETRY_DELAY = 60  # seconds
MAX_DAYS_PER_REQUEST = 100  # Maximum number of days per historical data request
MAX_REQUEST_PER_MINUTE = 5  # Rate limiting - max requests per minute
BATCH_SIZE = 10000  # Number of records to insert in a single ClickHouse batch

# Time zones
IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.UTC

# Commodity Exchange Mapping
EXCHANGE_MAP = {
    "MCX": "MCX",  # Changed from MCX_FO to MCX
    "NCDEX": "NCX"  # Changed from NCX_FO to NCX
}

# Hard-coded working contracts based on our successful test
WORKING_CONTRACTS = [
    # This contract was successfully tested and returns data
    ("GOLD03OCT25FUT", "MCX", "440939"),
    
    # These are futures contracts we found through search
    ("NATURALGAS25JUN25FUT", "MCX", "446265"),
    ("ALUMINIUM30MAY25FUT", "MCX", "446487"),
    ("NICKEL30MAY25FUT", "MCX", "446491"),
]

# Function to dynamically generate commodity list with latest tokens
def get_latest_commodity_tokens():
    """
    Try to load the latest commodity tokens from working_contract.json
    If the file doesn't exist, return the default tokens
    """
    import os
    import json
    
    # Start with our known working contracts
    commodity_list = WORKING_CONTRACTS.copy()
    
    # Try to load working contract information
    try:
        if os.path.exists('working_contract.json'):
            with open('working_contract.json', 'r') as f:
                data = json.load(f)
                if 'contract' in data:
                    contract = data['contract']
                    commodity_list.append((
                        contract['name'],
                        contract['exchange'],
                        contract['token']
                    ))
    except Exception:
        pass  # Ignore errors and use defaults
        
    # Check for specific contract files
    try:
        for filename in os.listdir('.'):
            if filename.startswith('working_specific_contract_') and filename.endswith('.json'):
                with open(filename, 'r') as f:
                    data = json.load(f)
                    if 'contract' in data:
                        contract = data['contract']
                        commodity_list.append((
                            contract['name'],
                            contract['exchange'],
                            contract['token']
                        ))
    except Exception:
        pass  # Ignore errors and use defaults
    
    return commodity_list

# Commodity List - will be populated with latest tokens when available
COMMODITY_LIST = get_latest_commodity_tokens()

# Supported intervals for historical data
INTERVALS = {
    "1min": "ONE_MINUTE",
    "5min": "FIVE_MINUTE",
    "15min": "FIFTEEN_MINUTE",
    "30min": "THIRTY_MINUTE",
    "1hour": "ONE_HOUR",
    "1day": "ONE_DAY"
}

# Default interval for fetching data
DEFAULT_INTERVAL = INTERVALS["1day"]

# Date range for historical data (5+ years)
END_DATE = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=365 * 5 + 30)  # 5 years and 1 month

# Logging Configuration
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"fetcher_{datetime.now().strftime('%Y%m%d')}.log")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"