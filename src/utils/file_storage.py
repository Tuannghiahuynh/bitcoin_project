"""
File Storage Manager
Handle saving and managing raw and processed data files
"""
import os
import json
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)


class FileStorageManager:
    """Manage data file storage with retention policies"""
    
    def __init__(self):
        self.raw_dir = 'data/raw'
        self.processed_dir = 'data/processed'
        self.raw_retention_days = 7
        self.processed_retention_days = 30
        
        # Ensure directories exist
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
    
    def save_raw_data(self, data, timestamp=None):
        """
        Save raw data as JSON file with metadata
        
        Args:
            data: Raw data from API (list or dict)
            timestamp: Optional timestamp for filename
        
        Returns:
            str: Path to saved file
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        filename = f"bitcoin_raw_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        try:
            # Format data with metadata for better readability
            formatted_data = {
                "metadata": {
                    "source": "Binance API",
                    "symbol": "BTCUSDT",
                    "interval": "30m",
                    "fetch_timestamp": timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    "total_records": len(data) if isinstance(data, list) else 1
                },
                "data": []
            }
            
            # Convert raw data to readable format
            if isinstance(data, list):
                for item in data:
                    formatted_data["data"].append({
                        "timestamp": item[0],
                        "timestamp_readable": datetime.fromtimestamp(item[0]/1000).strftime('%Y-%m-%d %H:%M:%S'),
                        "open": float(item[1]),
                        "high": float(item[2]),
                        "low": float(item[3]),
                        "close": float(item[4]),
                        "volume": float(item[5])
                    })
            else:
                formatted_data["data"] = data
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(formatted_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved raw data to {filename}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving raw data: {e}")
            raise
    
    def save_processed_data(self, df, date=None):
        """
        Save processed data as Parquet file
        Append to existing file if same date
        
        Args:
            df: Processed DataFrame
            date: Date for filename (default: today)
        
        Returns:
            str: Path to saved file
        """
        if date is None:
            date = datetime.now().date()
        
        filename = f"bitcoin_processed_{date.strftime('%Y%m%d')}.parquet"
        filepath = os.path.join(self.processed_dir, filename)
        
        try:
            # Check if file exists for today
            if os.path.exists(filepath):
                # Read existing data
                existing_df = pd.read_parquet(filepath)
                
                # Append new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                
                # Remove duplicates based on timestamp
                combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
                
                # Sort by timestamp
                combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
                
                # Save combined data
                combined_df.to_parquet(filepath, index=False)
                logger.info(f"Appended {len(df)} records to {filename} (total: {len(combined_df)})")
            else:
                # Save new file
                df.to_parquet(filepath, index=False)
                logger.info(f"Saved processed data to {filename} ({len(df)} records)")
            
            return filepath
        except Exception as e:
            logger.error(f"Error saving processed data: {e}")
            raise
    
    def cleanup_old_files(self):
        """
        Remove files older than retention period
        """
        now = datetime.now()
        
        # Cleanup raw files (7 days)
        self._cleanup_directory(
            self.raw_dir, 
            self.raw_retention_days, 
            now,
            'raw'
        )
        
        # Cleanup processed files (30 days)
        self._cleanup_directory(
            self.processed_dir, 
            self.processed_retention_days, 
            now,
            'processed'
        )
    
    def _cleanup_directory(self, directory, retention_days, current_time, file_type):
        """
        Cleanup old files in a directory
        
        Args:
            directory: Directory path
            retention_days: Number of days to retain
            current_time: Current datetime
            file_type: Type of files (for logging)
        """
        try:
            files = os.listdir(directory)
            deleted_count = 0
            
            for filename in files:
                filepath = os.path.join(directory, filename)
                
                # Skip if not a file
                if not os.path.isfile(filepath):
                    continue
                
                # Get file modification time
                file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))
                file_age = (current_time - file_mtime).days
                
                # Delete if older than retention period
                if file_age > retention_days:
                    os.remove(filepath)
                    deleted_count += 1
                    logger.info(f"Deleted old {file_type} file: {filename} (age: {file_age} days)")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old {file_type} files")
            else:
                logger.info(f"No old {file_type} files to clean up")
                
        except Exception as e:
            logger.error(f"Error cleaning up {file_type} files: {e}")
    
    def get_file_stats(self):
        """
        Get statistics about stored files
        
        Returns:
            dict: Statistics about raw and processed files
        """
        stats = {
            'raw': self._get_directory_stats(self.raw_dir),
            'processed': self._get_directory_stats(self.processed_dir)
        }
        return stats
    
    def _get_directory_stats(self, directory):
        """Get statistics for a directory"""
        try:
            files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
            
            if not files:
                return {'count': 0, 'total_size_mb': 0}
            
            total_size = sum(os.path.getsize(os.path.join(directory, f)) for f in files)
            
            return {
                'count': len(files),
                'total_size_mb': round(total_size / (1024 * 1024), 2)
            }
        except Exception as e:
            logger.error(f"Error getting directory stats: {e}")
            return {'count': 0, 'total_size_mb': 0}


def get_storage_manager():
    """
    Get file storage manager instance
    
    Returns:
        FileStorageManager: Storage manager instance
    """
    return FileStorageManager()
