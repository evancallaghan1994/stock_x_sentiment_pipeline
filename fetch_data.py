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

# Remove the testing section:
# TESTING: Testing script with small list of tickers
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
    # Add new key-value pair so we always know which ticker we're working with
    data['symbol'] = symbol
    
    # Add additional valuation metrics
    try:
        # Get additional metrics from the API response
        if 'chart' in data and 'result' in data['chart']:
            result = data['chart']['result'][0]
            
            # Add valuation metrics if available
            if 'meta' in result:
                meta = result['meta']
                data['pe_ratio'] = meta.get('trailingPE', None)
                data['market_cap'] = meta.get('marketCap', None)
                data['dividend_yield'] = meta.get('dividendYield', None)
                data['eps'] = meta.get('trailingEPS', None)
                data['pb_ratio'] = meta.get('priceToBook', None)
                data['ps_ratio'] = meta.get('priceToSales', None)
                
    except Exception as e:
        print(f"Warning: Could not extract additional metrics for {symbol}: {e}")
        # Set default values if extraction fails
        data['pe_ratio'] = None
        data['market_cap'] = None
        data['dividend_yield'] = None
        data['eps'] = None
        data['pb_ratio'] = None
        data['ps_ratio'] = None
    
    # Return the data
    return data

# Wrapper to fetch multiple tickers
# Returns a list of dictionaries, one per ticker
# Being async allows us to fetch all stocks concurrently using await and asyncio.gather
async def fetch_all_stocks(tickers):
    # initialize empty list to store all ticker JSON data
    all_data = []
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
                data = await fetch_stock_data(symbol, page)
                return data
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                return None
            finally:
                await page.close()
        
        # Use asyncio.gather to run all fetches concurrently
        results = await asyncio.gather(*[fetch_single_stock(symbol) for symbol in tickers])
        
        # Filter out None results (failed fetches) and add to all_data
        all_data = [data for data in results if data is not None]
        
        # Close the browser
        await browser.close()

    # Convert list of dictionaries to pandas DataFrame
    result_df = pd.DataFrame(all_data)
    return result_df
# Orchestrate the fetching process
async def main():
    # Fetch all stock data
    sp500_df = await fetch_all_stocks(tickers)
    # Quick preview of pandas DataFrame
    print("DataFrame shape:", sp500_df.shape)
    print("First 5 rows:")
    print(sp500_df.head())
    
    # Save to parquet local file
    local_parquet_path = 'sp500_stock_data.parquet'
    sp500_df.to_parquet(local_parquet_path, index=False)
    
    # Upload to Google Cloud Storage
    print("Uploading data to Google Cloud Storage...")
    gcs_path = upload_parquet_to_gcs(local_parquet_path)
    print(f"Data successfully uploaded to: {gcs_path}")
    
    # Optionally, clean up local file after upload
    # os.remove(local_parquet_path)
    # print("Local parquet file cleaned up")

# Python guard to ensure main() is only run when the script is executed directly
if __name__ == "__main__":
    asyncio.run(main())
