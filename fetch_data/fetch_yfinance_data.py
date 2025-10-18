"""
This script fetches stock data and twitter sentiment data for a given stock ticker.

"""
# First, we need to fetch the stock data.
# We're going to use playwright to fetch data from the web.
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Google Cloud Storage imports
from google.cloud import storage
from google.cloud.exceptions import NotFound

# Note: Using pandas for local processing, PySpark will be used in Databricks

# Google Cloud Storage configuration
load_dotenv()
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def upload_to_gcs(local_file_path, gcs_blob_name):
    """
    Upload a file to Google Cloud Storage bucket
    
    Args:
        local_file_path (str): Path to the local file to upload
        gcs_blob_name (str): Name to give the file in GCS bucket
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        
        # Get the bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Create a blob object
        blob = bucket.blob(gcs_blob_name)
        
        # Upload the file
        blob.upload_from_filename(local_file_path)
        
        print(f"Successfully uploaded {local_file_path} to gs://{BUCKET_NAME}/{gcs_blob_name}")
        
    except NotFound:
        print(f"Bucket {BUCKET_NAME} not found. Please check your bucket name and permissions.")
    except Exception as e:
        print(f"Error uploading to GCS: {e}")

def upload_parquet_to_gcs(local_parquet_path):
    """
    Upload parquet data to GCS with timestamped filename
    
    Args:
        local_parquet_path (str): Path to the local parquet file
    """
    # Generate timestamp for unique filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    gcs_filename = f"sp500_stock_data_{timestamp}.parquet"
    
    # Upload to GCS
    upload_to_gcs(local_parquet_path, gcs_filename)
    
    return f"gs://{BUCKET_NAME}/{gcs_filename}"

# Get list of S&P 500 tickers
tickers_df = pd.read_csv("sp500_tickers.csv")
tickers = tickers_df['Symbol'].tolist()

# Get full S&P 500 ticker list
# TESTING: Uncomment the line below to test with a small subset first
# tickers = ['AAPL', 'MSFT', 'AMZN']

# We use async so that we playwright can handle delays, loading, waits, etc.
# Function to fetch stock data for a given ticker
async def fetch_stock_data(symbol, page):
    # Yahoo Finance API endpoint - Updated for 5 years daily data
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=5y&interval=1d"
    # Navigate to the URL and wait for the page to load
    await page.goto(url)
    # Fetch HTML/JSON from the page
    content = await page.content()
    # The JSON is wrapped in <pre>...</pre>, so we need to extract it
    start = content.find('{') # find the first {
    end = content.rfind('}') + 1 # find the last }
    json_str = content[start:end] # slices out the JSON as a string
    # Convert string to Python dictionary
    data = json.loads(json_str)
    
    # Extract time series data from the JSON response
    stock_records = []
    
    try:
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
                
    except Exception as e:
        print(f"Error extracting time series data for {symbol}: {e}")
        return []
    
    # Return list of records (one per date)
    return stock_records

# Wrapper to fetch multiple tickers
# Returns a list of dictionaries, one per ticker-date combination
# Being async allows us to fetch all stocks concurrently using await and asyncio.gather
async def fetch_all_stocks(tickers):
    # initialize empty list to store all stock records
    all_records = []
    # Start playwright in async context
    # Launches a headless browswer and opens a new page
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # Create a coroutine for each ticker using a separate page
        async def fetch_single_stock(symbol):
            # 
            page = await browser.new_page()
            try:
                print(f"Fetching {symbol}")
                records = await fetch_stock_data(symbol, page)
                return records
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                return []
            finally:
                await page.close()
        
        # Use asyncio.gather to run all fetches concurrently
        results = await asyncio.gather(*[fetch_single_stock(symbol) for symbol in tickers])
        
        # Flatten all records from all tickers into a single list
        for ticker_records in results:
            if ticker_records:  # Only add non-empty lists
                all_records.extend(ticker_records)
        
        # Close the browser
        await browser.close()

    # Convert list of dictionaries to pandas DataFrame
    result_df = pd.DataFrame(all_records)
    return result_df
# Orchestrate the fetching process
async def main():
    # Fetch all stock data
    sp500_df = await fetch_all_stocks(tickers)
    
    # Quick preview of pandas DataFrame
    print("DataFrame shape:", sp500_df.shape)
    print("Columns:", sp500_df.columns.tolist())
    print("First 5 rows:")
    print(sp500_df.head())
    
    # Check for data quality issues
    print("\nData Quality Check:")
    print(f"Total records: {len(sp500_df)}")
    print(f"Unique symbols: {sp500_df['symbol'].nunique()}")
    print(f"Date range: {sp500_df['date'].min()} to {sp500_df['date'].max()}")
    
    # Check for null values
    print("\nNull value counts:")
    print(sp500_df.isnull().sum())
    
    # Check for static pricing (the main issue we're investigating)
    print("\n=== PRICE VOLATILITY ANALYSIS ===")
    for symbol in sp500_df['symbol'].unique()[:5]:  # Check first 5 symbols
        symbol_data = sp500_df[sp500_df['symbol'] == symbol]
        if len(symbol_data) > 1:
            unique_closes = symbol_data['close'].nunique()
            total_records = len(symbol_data)
            print(f"{symbol}: {unique_closes} unique prices out of {total_records} records")
            if unique_closes == 1:
                print(f"  ⚠️  STATIC PRICING DETECTED: All prices = {symbol_data['close'].iloc[0]}")
            else:
                price_range = f"{symbol_data['close'].min():.2f} - {symbol_data['close'].max():.2f}"
                print(f"  ✅ Price range: ${price_range}")
    
    # Overall volatility check
    total_unique_prices = sp500_df['close'].nunique()
    total_records = len(sp500_df)
    print(f"\nOVERALL: {total_unique_prices} unique prices out of {total_records} total records")
    if total_unique_prices < total_records * 0.1:  # Less than 10% unique prices
        print("⚠️  WARNING: Very low price volatility detected across all stocks!")
        print("   This suggests static pricing or data quality issues.")
    else:
        print("✅ Good price volatility detected across the dataset.")
    
    # Sample a few records to verify data looks correct
    print("\nSample records for AAPL (if available):")
    aapl_sample = sp500_df[sp500_df['symbol'] == 'AAPL'].head(3)
    if not aapl_sample.empty:
        print(aapl_sample[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']])
    else:
        print("No AAPL data found, showing first 3 records:")
        print(sp500_df.head(3)[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']])
    
    # Save to parquet local file
    local_parquet_path = 'sp500_stock_data.parquet'
    sp500_df.to_parquet(local_parquet_path, index=False)
    
    # Upload to Google Cloud Storage
    print("\nUploading data to Google Cloud Storage...")
    gcs_path = upload_parquet_to_gcs(local_parquet_path)
    print(f"Data successfully uploaded to: {gcs_path}")
    
    # Optionally, clean up local file after upload
    # os.remove(local_parquet_path)
    # print("Local parquet file cleaned up")

# Python guard to ensure main() is only run when the script is executed directly
if __name__ == "__main__":
    asyncio.run(main())
