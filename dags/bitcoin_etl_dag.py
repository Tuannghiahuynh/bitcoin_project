"""
Bitcoin ETL DAG
Scheduled to run every 30 minutes to fetch and process Bitcoin price data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add project to Python path
sys.path.insert(0, '/opt/airflow/bitcoin_project')

# Import ETL functions
from src.extract.bitcoin_api import extract_bitcoin_data
from src.transform.transform_data import transform_data
from src.load.load_to_db import load_to_database
from src.utils.file_storage import get_storage_manager
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'bitcoin_etl_pipeline',
    default_args=default_args,
    description='Bitcoin ETL Pipeline - Extract, Transform, Load',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    catchup=False,
    tags=['bitcoin', 'etl', 'crypto'],
)


def cleanup_old_files_task():
    """Task to cleanup old data files"""
    logger.info("Starting file cleanup task...")
    storage = get_storage_manager()
    storage.cleanup_old_files()
    logger.info("File cleanup completed")


def extract_task():
    """Task to extract Bitcoin data from Binance API"""
    logger.info("Starting extract task...")
    df = extract_bitcoin_data()
    
    # Save to temporary location for next task
    temp_file = '/tmp/bitcoin_raw_data.parquet'
    df.to_parquet(temp_file, index=False)
    
    logger.info(f"Extract completed: {len(df)} records saved to {temp_file}")
    return temp_file


def transform_task():
    """Task to transform Bitcoin data"""
    import pandas as pd
    
    logger.info("Starting transform task...")
    
    # Load data from previous task
    temp_file = '/tmp/bitcoin_raw_data.parquet'
    df = pd.read_parquet(temp_file)
    
    # Transform data
    transformed_df = transform_data(df)
    
    # Save transformed data
    temp_transformed_file = '/tmp/bitcoin_transformed_data.parquet'
    transformed_df.to_parquet(temp_transformed_file, index=False)
    
    logger.info(f"Transform completed: {len(transformed_df)} records")
    return temp_transformed_file


def load_task():
    """Task to load data into PostgreSQL"""
    import pandas as pd
    
    logger.info("Starting load task...")
    
    # Load transformed data
    temp_file = '/tmp/bitcoin_transformed_data.parquet'
    df = pd.read_parquet(temp_file)
    
    # Load to database
    rows_inserted = load_to_database(df)
    
    logger.info(f"Load completed: {rows_inserted} rows inserted")
    
    # Cleanup temp files
    os.remove('/tmp/bitcoin_raw_data.parquet')
    os.remove('/tmp/bitcoin_transformed_data.parquet')
    
    return rows_inserted


def report_statistics_task():
    """Task to report pipeline statistics"""
    storage = get_storage_manager()
    stats = storage.get_file_stats()
    
    logger.info("Pipeline Statistics:")
    logger.info(f"  Raw files: {stats['raw']['count']} ({stats['raw']['total_size_mb']} MB)")
    logger.info(f"  Processed files: {stats['processed']['count']} ({stats['processed']['total_size_mb']} MB)")


# Define tasks
cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files_task,
    dag=dag,
)

extract = PythonOperator(
    task_id='extract_bitcoin_data',
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_task,
    dag=dag,
)

load = PythonOperator(
    task_id='load_to_database',
    python_callable=load_task,
    dag=dag,
)

report = PythonOperator(
    task_id='report_statistics',
    python_callable=report_statistics_task,
    dag=dag,
)

# Set task dependencies
cleanup_task >> extract >> transform >> load >> report
