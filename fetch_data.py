"""
This script fetches stock data and twitter sentiment data for a given stock ticker.

"""
# First, we need to fetch the stock data.
# We're going to use playwright to fetch data from the web.
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from pyspark.sql import SparkSession
import json

# Initialize Spark session
spark = SparkSession.builder.appName('sp500_stock_data').getOrCreate()

# Get list of S&P 500 tickers
tickers_df = pd.read_csv("sp500_tickers.csv")
tickers = tickers_df['Symbol'].tolist()

# We use async so that we playwright can handle delays, loading, waits, etc.
# Function to fetch stock data for a given ticker
async def fetch_stock_data(symbol, page):
    # Yahoo Finance API endpoint
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1m"
    # Navigate to the URL and wait for the page to load
    await page.goto(url)
    # Fetch HTML/JON from the page
    content = await page.content()
    # The JSON is wrapped in <pre>...</pre>, so we need to extract it
    start = content.find('{') # find the first {
    end = content.rfind('}') + 1 # find the last }
    json_str = content[start:end] # slices out the JSON as a string
    # Convert string to Python dictionary
    data = json.loads(json_str)
    # Add new key-value pair so we always know which ticker we're working with
    data['symbol'] = symbol
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

    # Convert list of dictionaries to PySpark DataFrame
    result_df = spark.createDataFrame(all_data)
    return result_df
# Orchestrate the fetching process
async def main():
    # Fetch all stock data
    sp500_df = await fetch_all_stocks(tickers)
    # Quick preview of PySpark DataFrame
    sp500_df.show(5, truncate=False)
    # Save to parquet local file
    sp500_df.write.mode('overwrite').parquet('sp500_stock_data.parquet')

# Python guard to ensure main() is only run when the script is executed directly
if __name__ == "__main__":
    asyncio.run(main())
