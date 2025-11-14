"""
Airflow DAG for Market Data Pipeline
Orchestrates incremental data ingestion, validation, transformation, and loading
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from airflow_dags.utils import get_incremental_date_range
from airflow_dags.incremental_fetch import (
    fetch_prices_incremental,
    fetch_news_incremental,
    save_prices_to_gcs,
    save_news_to_gcs
)
import asyncio
import logging
import sys

# Import transformation scripts
sys.path.insert(0, str(project_root))
from transformation_scripts.transform_raw_stocks import main as transform_prices_main
from transformation_scripts.news_sentiment import main as transform_news_main

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'market_pipeline',
    default_args=default_args,
    description='Incremental market data pipeline: prices, news, validation, transformation, and loading',
    schedule_interval=None,  # Manual trigger only (or set to '@daily' for daily runs)
    start_date=days_ago(1),
    catchup=False,
    tags=['market_data', 'incremental', 'prices', 'news', 'sentiment'],
)


def get_date_ranges(**context):
    """
    Get incremental date ranges for prices and news.
    Stores results in XCom for downstream tasks.
    """
    logging.info("Calculating incremental date ranges...")
    
    # Get date ranges from BigQuery
    prices_start, prices_end = get_incremental_date_range(
        'stock_prices_silver',
        date_column='date',
        lookback_days=365
    )
    
    news_start, news_end = get_incremental_date_range(
        'news_sentiment_silver',
        date_column='date_key',
        lookback_days=365
    )
    
    logging.info(f"Prices date range: {prices_start} to {prices_end}")
    logging.info(f"News date range: {news_start} to {news_end}")
    
    # Push to XCom
    context['ti'].xcom_push(key='prices_start', value=prices_start.isoformat() if prices_start else None)
    context['ti'].xcom_push(key='prices_end', value=prices_end.isoformat() if prices_end else None)
    context['ti'].xcom_push(key='news_start', value=news_start.isoformat() if news_start else None)
    context['ti'].xcom_push(key='news_end', value=news_end.isoformat() if news_end else None)
    
    # Check if we need to run
    if prices_start is None and news_start is None:
        logging.warning("No new data to fetch. Both date ranges are None.")
        context['ti'].xcom_push(key='skip_pipeline', value=True)
    else:
        context['ti'].xcom_push(key='skip_pipeline', value=False)


def ingest_prices_task(**context):
    """Fetch incremental stock prices and save to GCS Bronze"""
    # Get date range from XCom
    ti = context['ti']
    prices_start_str = ti.xcom_pull(key='prices_start', task_ids='get_date_ranges')
    prices_end_str = ti.xcom_pull(key='prices_end', task_ids='get_date_ranges')
    skip = ti.xcom_pull(key='skip_pipeline', task_ids='get_date_ranges')
    
    if skip or not prices_start_str:
        logging.info("Skipping prices ingestion - no new data to fetch")
        return
    
    from datetime import date
    prices_start = date.fromisoformat(prices_start_str)
    prices_end = date.fromisoformat(prices_end_str)
    
    logging.info(f"Fetching prices from {prices_start} to {prices_end}")
    
    # Fetch prices
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(fetch_prices_incremental(prices_start, prices_end))
        
        if df.empty:
            logging.warning("No price data fetched")
            return
        
        # Save to GCS
        date_range_str = f"{prices_start.strftime('%Y%m%d')}_{prices_end.strftime('%Y%m%d')}"
        save_prices_to_gcs(df, date_range_str)
        
        logging.info(f"✅ Successfully ingested {len(df)} price records")
    finally:
        loop.close()


def ingest_news_task(**context):
    """Fetch incremental news articles and save to GCS Bronze"""
    # Get date range from XCom
    ti = context['ti']
    news_start_str = ti.xcom_pull(key='news_start', task_ids='get_date_ranges')
    news_end_str = ti.xcom_pull(key='news_end', task_ids='get_date_ranges')
    skip = ti.xcom_pull(key='skip_pipeline', task_ids='get_date_ranges')
    
    if skip or not news_start_str:
        logging.info("Skipping news ingestion - no new data to fetch")
        return
    
    from datetime import date
    news_start = date.fromisoformat(news_start_str)
    news_end = date.fromisoformat(news_end_str)
    
    logging.info(f"Fetching news from {news_start} to {news_end}")
    
    # Fetch news
    articles_by_ticker = fetch_news_incremental(news_start, news_end)
    
    if not articles_by_ticker:
        logging.warning("No news data fetched")
        return
    
    # Save to GCS
    save_news_to_gcs(articles_by_ticker)
    
    total_articles = sum(len(articles) for articles in articles_by_ticker.values())
    logging.info(f"✅ Successfully ingested {total_articles} news articles")


def validate_bronze_task(**context):
    """Run validation notebooks on Bronze layer data"""
    logging.info("Running Bronze layer validation...")
    
    # Run validation notebooks
    # Note: This assumes notebooks can be run via papermill or nbconvert
    # For now, we'll use a simple check
    logging.info("✅ Bronze validation complete (placeholder)")


# Note: transform_prices_task and transform_news_task are now BashOperators below
# They execute notebooks using jupyter nbconvert --execute


def load_bigquery_task(**context):
    """Load transformed data to BigQuery Silver tables"""
    logging.info("Loading data to BigQuery...")
    
    # This would load the transformed data to BigQuery
    # The notebooks already handle this, so this is mainly for orchestration
    logging.info("✅ BigQuery load complete (placeholder)")


def notify_success_task(**context):
    """Send success notification"""
    logging.info("✅ Pipeline completed successfully!")
    
    # Add notification logic here (email, Slack, etc.)
    ti = context['ti']
    prices_start = ti.xcom_pull(key='prices_start', task_ids='get_date_ranges')
    news_start = ti.xcom_pull(key='news_start', task_ids='get_date_ranges')
    
    logging.info(f"Processed data from: Prices={prices_start}, News={news_start}")


# Define tasks
get_date_ranges_task = PythonOperator(
    task_id='get_date_ranges',
    python_callable=get_date_ranges,
    dag=dag,
)

ingest_prices_task_op = PythonOperator(
    task_id='ingest_prices',
    python_callable=ingest_prices_task,
    dag=dag,
)

ingest_news_task_op = PythonOperator(
    task_id='ingest_news',
    python_callable=ingest_news_task,
    dag=dag,
)

validate_bronze_task_op = PythonOperator(
    task_id='validate_bronze',
    python_callable=validate_bronze_task,
    dag=dag,
)

# Transform tasks - Execute Python scripts
def transform_prices_task(**context):
    """Transform stock prices data"""
    logging.info("Starting stock prices transformation...")
    try:
        transform_prices_main()
        logging.info("✅ Stock prices transformation completed successfully")
    except Exception as e:
        logging.error(f"❌ Error in stock prices transformation: {e}")
        raise

def transform_news_task(**context):
    """Transform news data and run sentiment analysis"""
    logging.info("Starting news sentiment transformation...")
    try:
        transform_news_main()
        logging.info("✅ News sentiment transformation completed successfully")
    except Exception as e:
        logging.error(f"❌ Error in news sentiment transformation: {e}")
        raise

transform_prices_task_op = PythonOperator(
    task_id='transform_prices',
    python_callable=transform_prices_task,
    dag=dag,
)

transform_news_task_op = PythonOperator(
    task_id='transform_news',
    python_callable=transform_news_task,
    dag=dag,
)

load_bigquery_task_op = PythonOperator(
    task_id='load_bigquery',
    python_callable=load_bigquery_task,
    dag=dag,
)

notify_success_task_op = PythonOperator(
    task_id='notify_success',
    python_callable=notify_success_task,
    dag=dag,
)

# Define task dependencies
get_date_ranges_task >> [ingest_prices_task_op, ingest_news_task_op]

[ingest_prices_task_op, ingest_news_task_op] >> validate_bronze_task_op

validate_bronze_task_op >> [transform_prices_task_op, transform_news_task_op]

[transform_prices_task_op, transform_news_task_op] >> load_bigquery_task_op

load_bigquery_task_op >> notify_success_task_op

