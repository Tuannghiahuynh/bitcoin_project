"""
Database Connection Manager
Handle PostgreSQL database connections
"""
import psycopg2
from psycopg2 import Error
from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseConnection:
    """Manage PostgreSQL database connections"""
    
    def __init__(self):
        config = get_config()
        db_config = config.get_db_config()
        
        self.host = db_config['host']
        self.port = db_config['port']
        self.database = db_config['database']
        self.user = db_config['user']
        self.password = db_config['password']
        self.connection = None
    
    def connect(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info("Successfully connected to PostgreSQL database")
            return self.connection
        except Error as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


def get_db_connection():
    """
    Get a database connection
    
    Returns:
        DatabaseConnection: Database connection manager
    """
    return DatabaseConnection()
