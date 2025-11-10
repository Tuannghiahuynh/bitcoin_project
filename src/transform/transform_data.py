"""
Data Transformation Module
Transform and validate Bitcoin price data
"""
import pandas as pd
from src.utils.logger import get_logger
from src.utils.file_storage import get_storage_manager

logger = get_logger(__name__)


class DataTransformer:
    """Transform and validate Bitcoin price data"""
    
    def __init__(self):
        pass
    
    def clean_data(self, df):
        """
        Clean and validate data
        
        Args:
            df (pandas.DataFrame): Raw data
        
        Returns:
            pandas.DataFrame: Cleaned data
        """
        logger.info("Starting data transformation...")
        
        # Remove duplicates
        original_count = len(df)
        df = df.drop_duplicates(subset=['timestamp'])
        
        # Remove null values
        df = df.dropna()
        
        # Ensure price is positive
        df = df[df['price'] > 0]
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        removed = original_count - len(df)
        if removed > 0:
            logger.info(f"Removed {removed} invalid/duplicate records")
        
        logger.info(f"Data cleaned: {len(df)} records remain")
        
        return df
    
    def add_features(self, df):
        """
        Add additional features to the data
        
        Args:
            df (pandas.DataFrame): Cleaned data
        
        Returns:
            pandas.DataFrame: Data with additional features
        """
        # Add date column for easier querying
        df['date'] = df['timestamp'].dt.date
        
        # Add hour of day
        df['hour'] = df['timestamp'].dt.hour
        
        # Round price to 2 decimal places
        df['price'] = df['price'].round(2)
        
        logger.info("Added features: date, hour")
        
        return df
    
    def get_statistics(self, df):
        """
        Calculate and log data statistics
        
        Args:
            df (pandas.DataFrame): Data to analyze
        """
        stats = {
            'total_records': len(df),
            'min_price': df['price'].min(),
            'max_price': df['price'].max(),
            'avg_price': df['price'].mean(),
            'std_price': df['price'].std(),
        }
        
        logger.info("Data Statistics:")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  Price range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
        logger.info(f"  Average price: ${stats['avg_price']:.2f}")
        logger.info(f"  Std deviation: ${stats['std_price']:.2f}")
        
        return stats


def transform_data(df):
    """
    Main function to transform data
    
    Args:
        df (pandas.DataFrame): Raw Bitcoin data
    
    Returns:
        pandas.DataFrame: Transformed data
    """
    transformer = DataTransformer()
    
    # Clean data
    df = transformer.clean_data(df)
    
    # Add features
    df = transformer.add_features(df)
    
    # Get statistics
    transformer.get_statistics(df)
    
    # Save processed data to Parquet file
    storage = get_storage_manager()
    storage.save_processed_data(df)
    
    return df
