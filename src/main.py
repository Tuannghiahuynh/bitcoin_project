"""
Bitcoin ETL Pipeline - Main Entry Point
Extract, Transform, Load Bitcoin price data
"""
import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extract.bitcoin_api import extract_bitcoin_data
from src.transform.transform_data import transform_data
from src.load.load_to_db import load_to_database
from src.utils.config import get_config
from src.utils.logger import get_logger
from src.utils.file_storage import get_storage_manager

logger = get_logger(__name__)


def run_etl_pipeline(days=None):
    """
    Run the complete ETL pipeline
    
    Args:
        days (int): Number of days of data to fetch (uses config if not specified)
    
    Returns:
        dict: Pipeline execution results
    """
    # Load configuration
    config = get_config()
    if days is None:
        days = config.etl_fetch_days
    
    logger.info("="*70)
    logger.info("Starting Bitcoin ETL Pipeline")
    logger.info(f"Configuration: Fetching {days} days of data")
    logger.info("="*70)
    
    try:
        # Cleanup old files first
        logger.info("Cleaning up old files...")
        storage = get_storage_manager()
        storage.cleanup_old_files()
        
        # EXTRACT
        logger.info("Step 1/3: Extracting data from Binance API...")
        raw_data = extract_bitcoin_data(days)
        logger.info(f"Extraction completed: {len(raw_data)} records")
        
        # TRANSFORM
        logger.info("Step 2/3: Transforming data...")
        transformed_data = transform_data(raw_data)
        logger.info(f"Transformation completed: {len(transformed_data)} records")
        
        # LOAD
        logger.info("Step 3/3: Loading data to PostgreSQL...")
        rows_inserted = load_to_database(transformed_data)
        logger.info(f"Load completed: {rows_inserted} rows inserted")
        
        # Summary
        logger.info("="*70)
        logger.info("ETL Pipeline Completed Successfully")
        logger.info(f"  Total records fetched: {len(raw_data)}")
        logger.info(f"  Records after transform: {len(transformed_data)}")
        logger.info(f"  New records inserted: {rows_inserted}")
        
        # File storage stats
        stats = storage.get_file_stats()
        logger.info(f"  Raw files: {stats['raw']['count']} ({stats['raw']['total_size_mb']} MB)")
        logger.info(f"  Processed files: {stats['processed']['count']} ({stats['processed']['total_size_mb']} MB)")
        logger.info("="*70)
        
        return {
            'success': True,
            'records_fetched': len(raw_data),
            'records_transformed': len(transformed_data),
            'rows_inserted': rows_inserted
        }
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
        logger.info("="*70)
        return {
            'success': False,
            'error': str(e)
        }


if __name__ == "__main__":
    # Run the ETL pipeline (uses config.yaml settings)
    result = run_etl_pipeline()
    
    if not result['success']:
        exit(1)
