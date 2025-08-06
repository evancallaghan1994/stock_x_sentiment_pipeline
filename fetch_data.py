"""
This script fetches stock data and twitter sentiment data for a given stock ticker.

"""
# First, we need to fetch the stock data.
# We're going to use playwright to fetch data from the web.
import asyncio
from playwright.async_api import async_playwright

# We use async so that we playwright can handle delays, loading, waits, etc.
async def fetch_stock_data(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=1d&interval=1m"

    # Context manager to start and stop playwright
    async with async_playwright() as p:
        # Launch a headless chromium browser
        browser = await p.chromium.launch()
        # Open a new tab
        page = await browser.new_page()
        # Navigate to the URL and wait for the page to load
        await page.goto(url)
        # Fetch HTML/JON from the page
        content = await page.content()
        # Preview what Playwright fetched
        print(content)
        # Close browswer to free resources
        await browser.close()

# Sanity check to see if the script is working
# We need this to use asyncio.run() to run the coroutine
asyncio.run(fetch_stock_data("AAPL"))
    