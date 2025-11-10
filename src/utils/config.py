"""
Configuration Loader
Load application configuration from config.yaml and environment variables
"""
import yaml
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Application configuration"""
    
    def __init__(self):
        self._load_config()
        self._load_env()
    
    def _load_config(self):
        """Load configuration from config.yaml"""
        config_path = 'config/config.yaml'
        
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            # API Configuration
            api_config = config.get('api', {}).get('binance', {})
            self.api_base_url = api_config.get('base_url', 'https://api.binance.com/api/v3')
            self.api_symbol = api_config.get('symbol', 'BTCUSDT')
            self.api_interval = api_config.get('interval', '30m')
            self.api_limit = api_config.get('limit', 1000)
            
            # ETL Configuration
            etl_config = config.get('etl', {})
            self.etl_fetch_days = etl_config.get('fetch_days', 120)
            self.etl_batch_size = etl_config.get('batch_size', 500)
        else:
            # Default values if config file doesn't exist
            self.api_base_url = 'https://api.binance.com/api/v3'
            self.api_symbol = 'BTCUSDT'
            self.api_interval = '30m'
            self.api_limit = 1000
            self.etl_fetch_days = 120
            self.etl_batch_size = 500
    
    def _load_env(self):
        """Load database configuration from environment variables"""
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'bitcoin_db')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'postgres')
    
    def get_db_config(self):
        """Get database configuration as dictionary"""
        return {
            'host': self.db_host,
            'port': self.db_port,
            'database': self.db_name,
            'user': self.db_user,
            'password': self.db_password
        }
    
    def get_api_config(self):
        """Get API configuration as dictionary"""
        return {
            'base_url': self.api_base_url,
            'symbol': self.api_symbol,
            'interval': self.api_interval,
            'limit': self.api_limit
        }
    
    def get_etl_config(self):
        """Get ETL configuration as dictionary"""
        return {
            'fetch_days': self.etl_fetch_days,
            'batch_size': self.etl_batch_size
        }


# Singleton instance
_config = None


def get_config():
    """
    Get application configuration (Singleton pattern)
    
    Returns:
        Config: Application configuration instance
    """
    global _config
    if _config is None:
        _config = Config()
    return _config
