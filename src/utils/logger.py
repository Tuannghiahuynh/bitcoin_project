"""
Logging Configuration
Setup and manage application logging
"""
import logging
import logging.config
import logging.handlers
import yaml
import os
from datetime import datetime


class ImportantLogFilter(logging.Filter):
    """Filter to only log important messages to file"""
    
    # Keywords that indicate important log messages
    IMPORTANT_KEYWORDS = [
        'Starting Bitcoin ETL Pipeline',
        'Step 1/3', 'Step 2/3', 'Step 3/3',
        'Fetched', 'fetched',
        'Price range',
        'CDC:', 'Found',
        'Inserted', 'inserted',
        'Completed Successfully',
        'Total:',
        'Error', 'error',
        'Failed', 'failed',
        'Warning', 'warning'
    ]
    
    def filter(self, record):
        """
        Determine if the log record should be logged to file
        
        Args:
            record: LogRecord instance
        
        Returns:
            bool: True if should be logged, False otherwise
        """
        message = record.getMessage()
        
        # Always log ERROR and WARNING
        if record.levelno >= logging.WARNING:
            return True
        
        # Check if message contains important keywords
        for keyword in self.IMPORTANT_KEYWORDS:
            if keyword in message:
                return True
        
        # Filter out less important messages
        return False


def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if not exists
    os.makedirs('data/logs', exist_ok=True)
    
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create handlers
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename='data/logs/bitcoin_etl.log',
        when='midnight',
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)
    file_handler.addFilter(ImportantLogFilter())  # Add filter to file handler
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)


def get_logger(name):
    """
    Get a logger instance
    
    Args:
        name (str): Logger name (usually __name__)
    
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


# Initialize logging on module import
setup_logging()
