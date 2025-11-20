"""
Wrapper functions for incremental data fetching
These functions modify the existing fetch scripts to support date ranges
"""
import os
import sys
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fetch_data.fetch_sn import save_to_gcs, get_articles_for_day
from google.cloud import storage
from dotenv import load_dotenv
import pandas as pd
import logging

# Load environment variables
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path)

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

# Lazy initialization for storage client
_storage_client = None

def get_storage_client():
    """Get or create GCS storage client"""
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client


async def fetch_prices_incremental(start_date, end_date, tickers=None):
    """
    Fetch stock prices for a specific date range.
    
    Args:
        start_date: Start date (date object)
        end_date: End date (date object)
        tickers: List of tickers to fetch (None = all from stock_metadata.csv)
    
    Returns:
        pd.DataFrame: DataFrame with price data
    """
    from fetch_data.fetch_yfinance_data import tickers as default_tickers
    
    if tickers is None:
        tickers = default_tickers
    
    # Calculate days to fetch
    days_diff = (end_date - start_date).days + 1
    
    if days_diff <= 0:
        logging.warning(f"Invalid date range: {start_date} to {end_date}")
        return pd.DataFrame()
    
    logging.info(f"Fetching prices from {start_date} to {end_date} ({days_diff} days)")
    
    # For Yahoo Finance, we need to fetch a range that includes our dates
    # The API uses range parameters like '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max'
    # We'll fetch 1y and filter to the date range we need
    
    async def fetch_stock_data_with_filter(symbol, page):
        """Fetch stock data and filter to date range"""
        # Fetch 1 year of data (ensures we get our date range)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1y&interval=1d"
        
        try:
            from playwright.async_api import async_playwright
            import json
            
            await page.goto(url, timeout=30000)
            content = await page.content()
            start = content.find('{')
            end = content.rfind('}') + 1
            json_str = content[start:end]
            data = json.loads(json_str)
            
            stock_records = []
            
            if 'chart' in data and 'result' in data['chart'] and len(data['chart']['result']) > 0:
                result = data['chart']['result'][0]
                timestamps = result.get('timestamp', [])
                indicators = result.get('indicators', {})
                quote_data = indicators.get('quote', [{}])[0] if indicators.get('quote') else {}
                
                opens = quote_data.get('open', [])
                highs = quote_data.get('high', [])
                lows = quote_data.get('low', [])
                closes = quote_data.get('close', [])
                volumes = quote_data.get('volume', [])
                
                for i, timestamp in enumerate(timestamps):
                    date = datetime.fromtimestamp(timestamp).date()
                    
                    # Filter to date range
                    if start_date <= date <= end_date:
                        record = {
                            'symbol': symbol,
                            'date': date.strftime('%Y-%m-%d'),
                            'open': opens[i] if i < len(opens) and opens[i] is not None else None,
                            'high': highs[i] if i < len(highs) and highs[i] is not None else None,
                            'low': lows[i] if i < len(lows) and lows[i] is not None else None,
                            'close': closes[i] if i < len(closes) and closes[i] is not None else None,
                            'volume': volumes[i] if i < len(volumes) and volumes[i] is not None else None
                        }
                        stock_records.append(record)
        except Exception as e:
            logging.error(f"Error fetching {symbol}: {e}")
            return []
        
        return stock_records
    
    # Use the existing fetch_all_stocks pattern but with our filtered function
    from playwright.async_api import async_playwright
    
    all_records = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        async def fetch_single_stock(symbol):
            page = await browser.new_page()
            try:
                records = await fetch_stock_data_with_filter(symbol, page)
                return records
            except Exception as e:
                logging.error(f"Error fetching {symbol}: {e}")
                return []
            finally:
                await page.close()
        
        results = await asyncio.gather(*[fetch_single_stock(symbol) for symbol in tickers])
        
        for ticker_records in results:
            if ticker_records:
                all_records.extend(ticker_records)
        
        await browser.close()
    
    df = pd.DataFrame(all_records)
    return df


def fetch_news_incremental(start_date, end_date, tickers=None):
    """
    Fetch news articles for a specific date range.
    
    Args:
        start_date: Start date (date object)
        end_date: End date (date object)
        tickers: List of tickers to fetch (None = all from stock_metadata.csv)
    
    Returns:
        dict: Dictionary mapping ticker to list of articles
    """
    from fetch_data.fetch_sn import stock_df, tickers as default_tickers
    
    if tickers is None:
        tickers = default_tickers
    
    logging.info(f"Fetching news from {start_date} to {end_date}")
    
    all_articles_by_ticker = {}
    
    # Process each ticker
    for idx, row in stock_df.iterrows():
        ticker = row['Symbol']
        
        if ticker not in tickers:
            continue
        
        company_name = row.get('CompanyName', '')
        abbrev1 = row.get('Abbreviated Company Name', '') if pd.notna(row.get('Abbreviated Company Name', '')) else None
        abbrev2 = row.get('Abbreviated Secondary Company Name', '') if pd.notna(row.get('Abbreviated Secondary Company Name', '')) else None
        
        logging.info(f"Processing {ticker} ({company_name})")
        
        articles = []
        current_date = start_date
        
        # Process each day in range
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            # Fetch articles for this day
            day_articles, endpoint, search_terms = get_articles_for_day(
                ticker, current_date, company_name, abbrev1, abbrev2
            )
            
            # Add metadata
            for article in day_articles:
                article['ticker'] = ticker
                article['company_name'] = company_name
                article['query_date'] = date_str
                article['endpoint_used'] = endpoint
                article['search_terms_used'] = ', '.join(search_terms) if search_terms else 'ticker'
            
            articles.extend(day_articles)
            
            # Rate limiting
            import time
            time.sleep(0.25)  # API_DELAY
            
            current_date += timedelta(days=1)
        
        all_articles_by_ticker[ticker] = articles
        logging.info(f"✅ {ticker}: Collected {len(articles)} articles")
    
    return all_articles_by_ticker


def save_prices_to_gcs(df, date_range_str):
    """
    Save price data to GCS Bronze layer.
    
    Args:
        df: DataFrame with price data
        date_range_str: String identifier for date range (e.g., '20241114_20241115')
    """
    if df.empty:
        logging.warning("No price data to save")
        return
    
    bucket = get_storage_client().bucket(GCS_BUCKET_NAME)
    
    # Save per-ticker files
    for ticker in df['symbol'].unique():
        ticker_df = df[df['symbol'] == ticker]
        local_parquet = project_root / "data" / "yfinance_prices" / f"prices_{ticker}.parquet"
        local_parquet.parent.mkdir(parents=True, exist_ok=True)
        
        ticker_df.to_parquet(local_parquet, index=False)
        
        gcs_path = f"bronze/yfinance_prices/{ticker}/prices_{ticker}.parquet"
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_parquet))
        
        logging.info(f"✅ Uploaded {len(ticker_df)} records for {ticker} to GCS: {gcs_path}")
        
        # Clean up local file
        local_parquet.unlink()


def save_news_to_gcs(articles_by_ticker):
    """
    Save news articles to GCS Bronze layer.
    
    Args:
        articles_by_ticker: Dictionary mapping ticker to list of articles
    """
    for ticker, articles in articles_by_ticker.items():
        if articles:
            save_to_gcs(articles, ticker)

