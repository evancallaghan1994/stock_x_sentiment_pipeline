"""
This script fetches stock data and twitter sentiment data for a given stock ticker.

"""
# First, we need to fetch the stock data.
# We're going to use playwright to fetch data from the web.
import asyncio
from playwright.async_api import async_playwright
import pandas as pd

# Get list of S&P 500 tickers
import pandas as pd
tickers_df = pd.read_csv("sp500_tickers.csv")
tickers = tickers_df['Symbol'].tolist()

# We use async so that we playwright can handle delays, loading, waits, etc.
# Function to fetch stock data for a given ticker
async def fetch_stock_data(symbol, page):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1m"

    # Navigate to the URL and wait for the page to load
    await page.goto(url)
    # Fetch HTML/JON from the page
    content = await page.content()
    print(f"Data for {symbol}:")
    print(content[:500])  # print first 500 chars to avoid huge output

# Wrapper to fetch multiple tickers
async def fetch_all_stocks(tickers):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        for symbol in tickers:
            await fetch_stock_data(symbol, page)
        await browser.close()

# Run for 10 tickers to test
asyncio.run(fetch_all_stocks(tickers[:10]))  # example: only first 10 tickers for testing

