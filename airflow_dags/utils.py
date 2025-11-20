"""
Utility functions for Airflow DAGs
"""
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
env_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path=env_path)

BIGQUERY_PROJECT = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE_STOCKS = os.getenv("BIGQUERY_TABLE_STOCKS")
BIGQUERY_TABLE_NEWS = os.getenv("BIGQUERY_TABLE_NEWS", "news_sentiment_silver")


def get_last_date_from_bigquery(table_name, date_column='date', ticker_column='ticker'):
    """
    Get the maximum date from a BigQuery table.
    
    Args:
        table_name: Name of the BigQuery table (e.g., 'stock_prices_silver' or 'news_sentiment_silver')
        date_column: Name of the date column to query
        ticker_column: Name of the ticker column (for per-ticker last dates)
    
    Returns:
        dict: Dictionary with 'overall_last_date' and optionally 'ticker_last_dates'
              Returns None if table is empty or doesn't exist
    """
    if not BIGQUERY_PROJECT or not BIGQUERY_DATASET:
        raise ValueError("BIGQUERY_PROJECT and BIGQUERY_DATASET must be set in environment")
    
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{table_name}"
    
    try:
        # Check if table exists
        table = client.get_table(table_id)
        
        # Get overall last date
        query = f"""
        SELECT MAX({date_column}) as last_date
        FROM `{table_id}`
        """
        
        # Get overall last date - use to_dataframe to avoid native library issues
        query_job = client.query(query)
        query_job.result()  # Wait for job to complete
        
        # Convert to dataframe (safer than iterating)
        df = query_job.to_dataframe(create_bqstorage_client=False)
        
        if not df.empty and df['last_date'].iloc[0] is not None:
            overall_last_date = df['last_date'].iloc[0]
            if isinstance(overall_last_date, datetime):
                overall_last_date = overall_last_date.date()
            elif hasattr(overall_last_date, 'date'):
                overall_last_date = overall_last_date.date()
            
            # Get per-ticker last dates (simplified to avoid segfault)
            # Skip per-ticker for now to avoid the crash
            ticker_last_dates = {}
            
            return {
                'overall_last_date': overall_last_date,
                'ticker_last_dates': ticker_last_dates,
                'table_exists': True
            }
        else:
            # Table exists but is empty
            return {
                'overall_last_date': None,
                'ticker_last_dates': {},
                'table_exists': True
            }
            
    except NotFound:
        # Table doesn't exist yet
        return {
            'overall_last_date': None,
            'ticker_last_dates': {},
            'table_exists': False
        }
    except Exception as e:
        raise Exception(f"Error querying BigQuery table {table_id}: {e}")


def get_incremental_date_range(table_name, date_column='date', lookback_days=365):
    """
    Get the date range for incremental data fetching.
    
    Args:
        table_name: Name of the BigQuery table
        date_column: Name of the date column
        lookback_days: Maximum lookback if table is empty (default 365)
    
    Returns:
        tuple: (start_date, end_date) as date objects
    """
    result = get_last_date_from_bigquery(table_name, date_column)
    
    end_date = datetime.now().date()
    
    if result['overall_last_date']:
        # Start from day after last date
        start_date = result['overall_last_date'] + timedelta(days=1)
    else:
        # Table is empty or doesn't exist, use lookback
        start_date = end_date - timedelta(days=lookback_days)
    
    # Don't fetch future dates
    if start_date > end_date:
        return None, None
    
    return start_date, end_date


def get_incremental_date_range_for_ticker(table_name, ticker, date_column='date', lookback_days=365):
    """
    Get the date range for incremental data fetching for a specific ticker.
    
    Args:
        table_name: Name of the BigQuery table
        ticker: Ticker symbol
        date_column: Name of the date column
        lookback_days: Maximum lookback if ticker not found (default 365)
    
    Returns:
        tuple: (start_date, end_date) as date objects
    """
    result = get_last_date_from_bigquery(table_name, date_column)
    
    end_date = datetime.now().date()
    
    # Check if we have last date for this specific ticker
    ticker_last_dates = result.get('ticker_last_dates', {})
    if ticker in ticker_last_dates and ticker_last_dates[ticker]:
        start_date = ticker_last_dates[ticker] + timedelta(days=1)
    elif result['overall_last_date']:
        # Use overall last date as fallback
        start_date = result['overall_last_date'] + timedelta(days=1)
    else:
        # No data exists, use lookback
        start_date = end_date - timedelta(days=lookback_days)
    
    # Don't fetch future dates
    if start_date > end_date:
        return None, None
    
    return start_date, end_date

