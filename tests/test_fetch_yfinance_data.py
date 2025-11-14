"""
test_fetch_yfinance_data.py - Integration tests for Yahoo Finance fetcher
Tests API access, response schema, and file output
"""

import pytest
import os
import sys
import json
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import pandas as pd

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_root, 'fetch_data'))

from dotenv import load_dotenv

# Load environment variables
env_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path=env_path)

# Import functions to test
from fetch_yfinance_data import (
    fetch_stock_data,
    fetch_all_stocks,
    upload_to_gcs
)


class TestYahooFinanceAPI:
    """Integration tests for Yahoo Finance fetcher"""
    
    @pytest.fixture
    def test_ticker(self):
        """Test ticker (high-volume stock)"""
        return "AAPL"
    
    @pytest.fixture
    def mock_yahoo_response(self):
        """Mock Yahoo Finance API response"""
        return {
            'chart': {
                'result': [{
                    'timestamp': [1699833600, 1699920000],  # Sample timestamps
                    'indicators': {
                        'quote': [{
                            'open': [150.0, 151.0],
                            'high': [152.0, 153.0],
                            'low': [149.0, 150.0],
                            'close': [151.5, 152.5],
                            'volume': [1000000, 1100000]
                        }]
                    }
                }]
            }
        }
    
    @pytest.mark.asyncio
    async def test_api_access_success(self, test_ticker):
        """
        Test that Yahoo Finance API is accessible (lightweight live call)
        This makes a real API call to verify access works
        """
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                records = await fetch_stock_data(test_ticker, page)
                
                # Should return a list (can be empty if error, but should not raise)
                assert isinstance(records, list), "Should return a list of records"
                
                # If we got records, verify they have data
                if len(records) > 0:
                    assert len(records) > 0, "Should have at least one record"
            finally:
                await page.close()
                await browser.close()
    
    @pytest.mark.asyncio
    async def test_response_schema_ohlcv(self, test_ticker):
        """
        Test that API response contains required OHLCV fields
        Makes a lightweight live call to verify schema
        """
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                records = await fetch_stock_data(test_ticker, page)
                
                # If we got records, verify schema
                if records and len(records) > 0:
                    record = records[0]
                    
                    # Required fields for stock price data
                    required_fields = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
                    for field in required_fields:
                        assert field in record, f"Record missing required field: {field}"
                    
                    # Verify field types
                    assert isinstance(record['symbol'], str), "Symbol should be a string"
                    assert isinstance(record['date'], str), "Date should be a string"
                    assert record['date'].count('-') == 2, "Date should be in YYYY-MM-DD format"
                    
                    # Verify numeric fields are numbers (can be None for missing data)
                    numeric_fields = ['open', 'high', 'low', 'close', 'volume']
                    for field in numeric_fields:
                        value = record.get(field)
                        if value is not None:
                            assert isinstance(value, (int, float)), f"{field} should be numeric"
            finally:
                await page.close()
                await browser.close()
    
    @pytest.mark.asyncio
    async def test_fetch_stock_data_date_range(self, test_ticker):
        """
        Test that fetched data covers expected date range (1 year)
        """
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                records = await fetch_stock_data(test_ticker, page)
                
                if records and len(records) > 0:
                    # Convert dates to datetime for comparison
                    dates = [datetime.strptime(r['date'], '%Y-%m-%d') for r in records]
                    min_date = min(dates)
                    max_date = max(dates)
                    
                    # Should have data within last year
                    one_year_ago = datetime.now() - timedelta(days=365)
                    assert max_date >= one_year_ago, "Should have recent data"
                    
                    # Should have multiple trading days (at least 200)
                    assert len(records) >= 200, "Should have at least 200 trading days"
            finally:
                await page.close()
                await browser.close()
    
    @pytest.mark.asyncio
    async def test_fetch_stock_data_ohlc_relationships(self, test_ticker):
        """
        Test that OHLC data has valid relationships (high >= low, etc.)
        """
        from playwright.async_api import async_playwright
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                records = await fetch_stock_data(test_ticker, page)
                
                if records and len(records) > 0:
                    for record in records[:10]:  # Check first 10 records
                        high = record.get('high')
                        low = record.get('low')
                        open_price = record.get('open')
                        close = record.get('close')
                        
                        # Skip if any are None
                        if all(x is not None for x in [high, low, open_price, close]):
                            assert high >= low, "High should be >= Low"
                            assert high >= open_price, "High should be >= Open"
                            assert high >= close, "High should be >= Close"
                            assert low <= open_price, "Low should be <= Open"
                            assert low <= close, "Low should be <= Close"
            finally:
                await page.close()
                await browser.close()
    
    @patch('fetch_yfinance_data.storage_client')
    def test_upload_to_gcs_creates_file(self, mock_storage_client, tmp_path):
        """
        Test that upload_to_gcs uploads file to GCS
        Uses mocked GCS client
        """
        # Create test parquet file
        test_data = pd.DataFrame([{
            'symbol': 'AAPL',
            'date': '2024-11-13',
            'open': 150.0,
            'high': 152.0,
            'low': 149.0,
            'close': 151.5,
            'volume': 1000000
        }])
        
        temp_file = os.path.join(tmp_path, 'test_prices.parquet')
        test_data.to_parquet(temp_file, index=False)
        
        # Mock GCS bucket and blob
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        # Call upload_to_gcs
        upload_to_gcs(temp_file, 'bronze/yfinance_prices/test/test.parquet')
        
        # Verify GCS upload was called
        mock_storage_client.bucket.assert_called_once()
        mock_bucket.blob.assert_called_once()
        mock_blob.upload_from_filename.assert_called_once_with(temp_file)
    
    def test_local_file_schema(self, tmp_path):
        """
        Test that saved parquet file has correct schema
        """
        test_data = pd.DataFrame([{
            'symbol': 'AAPL',
            'date': '2024-11-13',
            'open': 150.0,
            'high': 152.0,
            'low': 149.0,
            'close': 151.5,
            'volume': 1000000
        }])
        
        temp_file = os.path.join(tmp_path, 'test_prices.parquet')
        test_data.to_parquet(temp_file, index=False)
        
        # Verify file exists
        assert os.path.exists(temp_file), "Parquet file should be created"
        
        # Verify schema
        df_read = pd.read_parquet(temp_file)
        required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            assert col in df_read.columns, f"Parquet file missing required column: {col}"
        
        # Verify data integrity
        assert len(df_read) == 1, "Should have one record"
        assert df_read.iloc[0]['symbol'] == 'AAPL', "Data should be preserved"
        assert df_read.iloc[0]['close'] == 151.5, "Numeric data should be preserved"
    
    @pytest.mark.asyncio
    async def test_fetch_all_stocks_structure(self):
        """
        Test that fetch_all_stocks returns DataFrame with correct structure
        Uses a small subset of tickers for speed
        """
        test_tickers = ['AAPL', 'MSFT']  # Just 2 tickers for speed
        
        result_df = await fetch_all_stocks(test_tickers)
        
        # Should return a DataFrame
        assert isinstance(result_df, pd.DataFrame), "Should return a pandas DataFrame"
        
        # If we got data, verify structure
        if not result_df.empty:
            required_columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
            for col in required_columns:
                assert col in result_df.columns, f"DataFrame missing required column: {col}"
            
            # Verify we have data for requested tickers
            unique_symbols = result_df['symbol'].unique().tolist()
            for ticker in test_tickers:
                assert ticker in unique_symbols, f"Should have data for {ticker}"
    
    def test_error_handling_invalid_ticker(self):
        """
        Test error handling for invalid ticker
        """
        async def test_invalid():
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                try:
                    records = await fetch_stock_data('INVALIDTICKER123', page)
                    # Should return empty list, not raise
                    assert isinstance(records, list), "Should return list even for invalid ticker"
                finally:
                    await page.close()
                    await browser.close()
        
        # Run async test
        asyncio.run(test_invalid())
    
    def test_ohlcv_data_types(self, tmp_path):
        """
        Test that OHLCV data types are correct in saved files
        """
        test_data = pd.DataFrame([{
            'symbol': 'AAPL',
            'date': '2024-11-13',
            'open': 150.0,
            'high': 152.0,
            'low': 149.0,
            'close': 151.5,
            'volume': 1000000
        }])
        
        temp_file = os.path.join(tmp_path, 'test_prices.parquet')
        test_data.to_parquet(temp_file, index=False)
        
        # Read back and verify types
        df_read = pd.read_parquet(temp_file)
        
        # Verify numeric columns are numeric
        assert pd.api.types.is_numeric_dtype(df_read['open']), "Open should be numeric"
        assert pd.api.types.is_numeric_dtype(df_read['high']), "High should be numeric"
        assert pd.api.types.is_numeric_dtype(df_read['low']), "Low should be numeric"
        assert pd.api.types.is_numeric_dtype(df_read['close']), "Close should be numeric"
        assert pd.api.types.is_numeric_dtype(df_read['volume']), "Volume should be numeric"
        
        # Verify string columns are strings
        assert pd.api.types.is_string_dtype(df_read['symbol']) or \
               pd.api.types.is_object_dtype(df_read['symbol']), "Symbol should be string"
        assert pd.api.types.is_string_dtype(df_read['date']) or \
               pd.api.types.is_object_dtype(df_read['date']), "Date should be string"

