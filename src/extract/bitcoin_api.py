"""
Bitcoin API Extractor
Extract Bitcoin price data from Binance API
"""
import requests
import pandas as pd
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.file_storage import get_storage_manager

logger = get_logger(__name__)


class BitcoinExtractor:
    """Extract Bitcoin price data from Binance API"""
    
    def __init__(self):
        config = get_config()
        api_config = config.get_api_config()
        
        self.base_url = f"{api_config['base_url']}/klines"
        self.symbol = api_config['symbol']
        self.interval = api_config['interval']
        self.max_limit = api_config['limit']
    
    def fetch_prices(self, days=None):
        """
        Fetch Bitcoin prices for the specified number of days
        
        Args:
            days (int): Number of days to fetch (uses config if not specified)
        
        Returns:
            pandas.DataFrame: DataFrame with timestamp and price columns
        """
        if days is None:
            config = get_config()
            days = config.etl_fetch_days
        
        # Calculate limit (48 intervals per day for 30m intervals)
        limit = min(days * 48, self.max_limit)
        
        params = {
            'symbol': self.symbol,
            'interval': self.interval,
            'limit': limit
        }
        
        try:
            logger.info(f"Fetching Bitcoin data from Binance API (limit: {limit} records)")
            
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Save raw data to JSON file
            storage = get_storage_manager()
            storage.save_raw_data(data)
            
            # Binance returns: [timestamp, open, high, low, close, volume, ...]
            # Extract timestamp and close price
            prices = [[item[0], float(item[4])] for item in data]
            
            # Convert to DataFrame
            df = pd.DataFrame(prices, columns=['timestamp_ms', 'price'])
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp_ms'], unit='ms')
            df = df[['timestamp', 'price']]
            
            logger.info(f"Successfully fetched {len(df)} records")
            logger.info(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Bitcoin data: {e}")
            raise
        except (KeyError, ValueError) as e:
            logger.error(f"Error parsing Bitcoin data: {e}")
            raise


def extract_bitcoin_data(days=None):
    """
    Main function to extract Bitcoin data
    
    Args:
        days (int): Number of days to fetch (uses config if not specified)
    
    Returns:
        pandas.DataFrame: Bitcoin price data
    """
    extractor = BitcoinExtractor()
    return extractor.fetch_prices(days)
