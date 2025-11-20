"""
fetch_yfinance_data.py - Fetch historical daily OHLCV data from Yahoo Finance
───────────────────────────────────────────────────────────────────────────────
Fetches 1 year of daily OHLCV (Open, High, Low, Close, Volume) data for all
stocks/ETFs in config/stock_metadata.csv and uploads to GCS Bronze layer.
Also saves files locally to data/ folder for offline processing.
"""

import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import json
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from tqdm import tqdm

# Google Cloud Storage imports
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
env_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path=env_path)

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
if not BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME not found in .env file")

# Initialize GCS client
storage_client = storage.Client()

def upload_to_gcs(local_file_path, gcs_blob_name):
    """
    Upload a file to Google Cloud Storage bucket
    
    Args:
        local_file_path (str): Path to the local file to upload
        gcs_blob_name (str): Name to give the file in GCS bucket
    """
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_blob_name)
        blob.upload_from_filename(local_file_path)
        print(f"✅ Uploaded to gs://{BUCKET_NAME}/{gcs_blob_name}")
    except NotFound:
        print(f"❌ Bucket {BUCKET_NAME} not found. Please check your bucket name and permissions.")
    except Exception as e:
        print(f"❌ Error uploading to GCS: {e}")

# Load stock metadata
config_path = os.path.join(project_root, "config", "stock_metadata.csv")
stock_df = pd.read_csv(config_path)
tickers = stock_df['Symbol'].tolist()

print("=" * 70)
print("YAHOO FINANCE DATA COLLECTION")
print("=" * 70)
print(f"Total stocks/ETFs: {len(tickers)}")
print(f"Date range: 1 year (365 days)")
print(f"Data: Daily OHLCV (Open, High, Low, Close, Volume)")
print("=" * 70)
print()

# Function to fetch stock data for a given ticker (1 year)
async def fetch_stock_data(symbol, page):
    """
    Fetch 1 year of daily OHLCV data for a given ticker from Yahoo Finance API.
    
    Args:
        symbol (str): Stock ticker symbol
        page: Playwright page object
    
    Returns:
        list: List of dictionaries, one per date with OHLCV data
    """
    # Yahoo Finance API endpoint - 1 year daily data
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1y&interval=1d"
    
    try:
        # Navigate to the URL and wait for the page to load
        await page.goto(url, timeout=30000)
        # Fetch HTML/JSON from the page
        content = await page.content()
        # The JSON is wrapped in <pre>...</pre>, so we need to extract it
        start = content.find('{')  # find the first {
        end = content.rfind('}') + 1  # find the last }
        json_str = content[start:end]  # slices out the JSON as a string
        # Convert string to Python dictionary
        data = json.loads(json_str)
        
        # Extract time series data from the JSON response
        stock_records = []
        
        if 'chart' in data and 'result' in data['chart'] and len(data['chart']['result']) > 0:
            result = data['chart']['result'][0]
            
            # Get timestamps and price data
            timestamps = result.get('timestamp', [])
            indicators = result.get('indicators', {})
            quote_data = indicators.get('quote', [{}])[0] if indicators.get('quote') else {}
            
            # Extract OHLCV data
            opens = quote_data.get('open', [])
            highs = quote_data.get('high', [])
            lows = quote_data.get('low', [])
            closes = quote_data.get('close', [])
            volumes = quote_data.get('volume', [])
            
            # Create records for each date
            for i, timestamp in enumerate(timestamps):
                # Convert Unix timestamp to datetime
                date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                
                record = {
                    'symbol': symbol,
                    'date': date,
                    'open': opens[i] if i < len(opens) and opens[i] is not None else None,
                    'high': highs[i] if i < len(highs) and highs[i] is not None else None,
                    'low': lows[i] if i < len(lows) and lows[i] is not None else None,
                    'close': closes[i] if i < len(closes) and closes[i] is not None else None,
                    'volume': volumes[i] if i < len(volumes) and volumes[i] is not None else None
                }
                
                stock_records.append(record)
        else:
            print(f"⚠️  No data found for {symbol}")
            return []
            
    except Exception as e:
        print(f"❌ Error fetching {symbol}: {e}")
        return []
    
    return stock_records

# Wrapper to fetch multiple tickers
async def fetch_all_stocks(tickers):
    """
    Fetch data for all tickers concurrently.
    
    Args:
        tickers (list): List of ticker symbols
    
    Returns:
        pd.DataFrame: DataFrame with all stock data
    """
    all_records = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        async def fetch_single_stock(symbol):
            page = await browser.new_page()
            try:
                records = await fetch_stock_data(symbol, page)
                return records
            except Exception as e:
                print(f"❌ Error fetching {symbol}: {e}")
                return []
            finally:
                await page.close()
        
        # Use asyncio.gather to run all fetches concurrently
        print(f"Fetching data for {len(tickers)} tickers...")
        results = await asyncio.gather(*[fetch_single_stock(symbol) for symbol in tickers])
        
        # Flatten all records from all tickers into a single list
        for ticker_records in results:
            if ticker_records:
                all_records.extend(ticker_records)
        
        await browser.close()
    
    # Convert list of dictionaries to pandas DataFrame
    result_df = pd.DataFrame(all_records)
    return result_df

# Orchestrate the fetching process
async def main():
    """
    Main function to fetch all stock data, save locally, and upload to GCS.
    """
    try:
        # Fetch all stock data
        print("Starting data collection...")
        stock_df = await fetch_all_stocks(tickers)
        
        if stock_df.empty:
            print("❌ No data collected!")
            return
        
        # Data quality checks
        print("\n" + "=" * 70)
        print("DATA QUALITY CHECK")
        print("=" * 70)
        print(f"Total records: {len(stock_df):,}")
        print(f"Unique symbols: {stock_df['symbol'].nunique()}")
        print(f"Date range: {stock_df['date'].min()} to {stock_df['date'].max()}")
        
        # Check for null values
        print("\nNull value counts:")
        null_counts = stock_df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                print(f"  {col}: {count} ({count/len(stock_df)*100:.1f}%)")
        
        # Records per ticker
        print("\nRecords per ticker:")
        ticker_counts = stock_df.groupby('symbol').size().sort_values(ascending=False)
        for ticker, count in ticker_counts.items():
            print(f"  {ticker}: {count} records")
        
        # Setup local directories
        local_data_dir = os.path.join(project_root, "data")
        local_prices_dir = os.path.join(local_data_dir, "yfinance_prices")
        os.makedirs(local_data_dir, exist_ok=True)
        os.makedirs(local_prices_dir, exist_ok=True)
        
        # Save combined file locally
        local_parquet_path = os.path.join(local_data_dir, 'yfinance_prices_1year.parquet')
        stock_df.to_parquet(local_parquet_path, index=False)
        print(f"\n✅ Saved combined file locally: {local_parquet_path}")
        
        # Save per-ticker files locally (mirroring GCS structure)
        print("\n" + "=" * 70)
        print("SAVING PER-TICKER FILES LOCALLY")
        print("=" * 70)
        for ticker in tqdm(tickers, desc="Saving locally", unit="ticker"):
            ticker_data = stock_df[stock_df['symbol'] == ticker]
            if not ticker_data.empty:
                # Create ticker-specific directory
                ticker_dir = os.path.join(local_prices_dir, ticker)
                os.makedirs(ticker_dir, exist_ok=True)
                
                # Save ticker-specific parquet
                ticker_parquet = os.path.join(ticker_dir, f"prices_{ticker}.parquet")
                ticker_data.to_parquet(ticker_parquet, index=False)
        
        print(f"✅ Saved per-ticker files to: {local_prices_dir}/")
        
        # Upload to GCS Bronze layer
        print("\n" + "=" * 70)
        print("UPLOADING TO GCS")
        print("=" * 70)
        
        # Upload combined file
        gcs_path_single = "bronze/yfinance_prices/yfinance_prices_1year.parquet"
        upload_to_gcs(local_parquet_path, gcs_path_single)
        
        # Upload per-ticker files
        print("\nUploading per-ticker files...")
        for ticker in tqdm(tickers, desc="Uploading", unit="ticker"):
            ticker_data = stock_df[stock_df['symbol'] == ticker]
            if not ticker_data.empty:
                # Local file path (already saved above)
                ticker_parquet = os.path.join(local_prices_dir, ticker, f"prices_{ticker}.parquet")
                
                # Upload to GCS
                gcs_path_ticker = f"bronze/yfinance_prices/{ticker}/prices_{ticker}.parquet"
                upload_to_gcs(ticker_parquet, gcs_path_ticker)
        
        print("\n" + "=" * 70)
        print("✅ DATA COLLECTION COMPLETE")
        print("=" * 70)
        print(f"Total records: {len(stock_df):,}")
        print(f"\nLocal files saved to:")
        print(f"  Combined: {local_parquet_path}")
        print(f"  Per-ticker: {local_prices_dir}/{{TICKER}}/prices_{{TICKER}}.parquet")
        print(f"\nGCS files uploaded to:")
        print(f"  gs://{BUCKET_NAME}/bronze/yfinance_prices/")
        print("=" * 70)
    
    except Exception as e:
        print(f"\n❌ Error in main(): {e}")
        import traceback
        traceback.print_exc()

# Python guard to ensure main() is only run when the script is executed directly
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️  Script interrupted by user")
    except Exception as e:
        print(f"\n❌ Error running script: {e}")
        import traceback
        traceback.print_exc()