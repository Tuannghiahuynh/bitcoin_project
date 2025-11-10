"""
Load Module
Load transformed data into PostgreSQL database
"""
import pandas as pd
from psycopg2 import Error
from src.utils.db_connection import get_db_connection
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataLoader:
    """Load data into PostgreSQL database"""
    
    def __init__(self):
        self.table_name = 'bitcoin_prices'
    
    def create_table(self, connection):
        """
        Create bitcoin_prices table if not exists
        
        Args:
            connection: PostgreSQL connection
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bitcoin_prices (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP UNIQUE NOT NULL,
            price NUMERIC(15, 2) NOT NULL,
            date DATE,
            hour INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_timestamp ON bitcoin_prices(timestamp);
        CREATE INDEX IF NOT EXISTS idx_date ON bitcoin_prices(date);
        """
        
        try:
            cursor = connection.cursor()
            cursor.execute(create_table_query)
            connection.commit()
            cursor.close()
            logger.info(f"Table '{self.table_name}' is ready")
        except Error as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    def get_latest_timestamp(self, connection):
        """
        Get the latest timestamp from database (CDC)
        
        Args:
            connection: PostgreSQL connection
        
        Returns:
            datetime: Latest timestamp or None
        """
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT MAX(timestamp) FROM {self.table_name}")
            result = cursor.fetchone()
            cursor.close()
            
            latest = result[0] if result[0] else None
            if latest:
                logger.info(f"Latest timestamp in database: {latest}")
            else:
                logger.info("No existing data in database")
            
            return latest
        except Error as e:
            logger.error(f"Error getting latest timestamp: {e}")
            return None
    
    def filter_new_data(self, df, latest_timestamp):
        """
        Filter only new data based on CDC
        
        Args:
            df (pandas.DataFrame): All data
            latest_timestamp: Latest timestamp in database
        
        Returns:
            pandas.DataFrame: Only new records
        """
        if latest_timestamp:
            original_count = len(df)
            df = df[df['timestamp'] > latest_timestamp]
            logger.info(f"CDC: Found {len(df)} new records (filtered from {original_count})")
        else:
            logger.info(f"No existing data - will insert all {len(df)} records")
        
        return df
    
    def insert_data(self, connection, df):
        """
        Insert data into database
        
        Args:
            connection: PostgreSQL connection
            df (pandas.DataFrame): Data to insert
        
        Returns:
            int: Number of rows inserted
        """
        if df.empty:
            logger.info("No new data to insert")
            return 0
        
        try:
            cursor = connection.cursor()
            
            insert_query = """
            INSERT INTO bitcoin_prices (timestamp, price, date, hour)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timestamp) 
            DO UPDATE SET 
                price = EXCLUDED.price,
                date = EXCLUDED.date,
                hour = EXCLUDED.hour;
            """
            
            # Prepare data for insertion
            data_to_insert = [
                (row['timestamp'], row['price'], row['date'], row['hour'])
                for _, row in df.iterrows()
            ]
            
            # Execute batch insert
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            
            rows_inserted = cursor.rowcount
            logger.info(f"Successfully inserted {rows_inserted} rows into {self.table_name}")
            
            cursor.close()
            return rows_inserted
            
        except Error as e:
            logger.error(f"Error inserting data: {e}")
            connection.rollback()
            raise
    
    def load(self, df):
        """
        Main load function
        
        Args:
            df (pandas.DataFrame): Transformed data to load
        
        Returns:
            int: Number of rows inserted
        """
        db_conn = get_db_connection()
        
        with db_conn as connection:
            # Create table
            self.create_table(connection)
            
            # Get latest timestamp (CDC)
            latest_timestamp = self.get_latest_timestamp(connection)
            
            # Filter new data
            df = self.filter_new_data(df, latest_timestamp)
            
            # Insert data
            rows_inserted = self.insert_data(connection, df)
        
        return rows_inserted


def load_to_database(df):
    """
    Main function to load data to database
    
    Args:
        df (pandas.DataFrame): Transformed data
    
    Returns:
        int: Number of rows inserted
    """
    loader = DataLoader()
    return loader.load(df)
