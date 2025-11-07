"""
test_fetch_reddit_pipeline.py
────────────────────────────────────────────────────────────────────────
Author: Evan Callaghan
Created: 2025-10-30
Description: 


License:
    All rights reserved. This repository is publicly accessible 
    for educational and portfolio demonstration purposes only.
"""
# ======================================================================
# Imports
# ======================================================================

# ----------------------------------------------------------------------
# Standard Imports
# ----------------------------------------------------------------------
import os
import sys
from datetime import datetime
import logging

# ----------------------------------------------------------------------
# Third-party Imports
# ----------------------------------------------------------------------
import pandas as pd # Data manipulation and analysis
from google.cloud import storage # For interacting with Google Cloud
import pytest # For testing

# ----------------------------------------------------------------------
# Local Imports
# ----------------------------------------------------------------------
# Add the project root to the Python path so we can import 
# the project modules.
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..")))

from fetch_data.fetch_reddit import (
    collect_for_ticker,
    run_reddit_ingestion,
    asset_meta,
    bucket_name,
)

# ======================================================================
# Logging Configuration
# ======================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ======================================================================
# Fixtures
# ======================================================================
# Define reusable test parameters (sample size and GCS test path) 
# for consistent Reddit ingestion testing.

@pytest.fixture(scope="module")
def sample_size():
    return 3

@pytest.fixture(scope="module")
def test_gcs_path():
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    return f"bronze/reddit/ingest_date={date_str}/reddit_raw.parquet"

# ======================================================================
# Integration Test: Full Reddit Ingestion (Small Sample)
# ======================================================================
# Run full Reddit ingestion pipeline with a small sample size to
# verify end-to-end functionality.
def test_reddit_ingestion(sample_size, test_gcs_path):

    # Run the ingestion pipeline with testing sample size.
    run_reddit_ingestion(sample_n=sample_size)

    # Verify upload to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=os.path.dirname(test_gcs_path)))

    # Check that the expected output file exists in GCS.
    assert any(b.name == test_gcs_path for b in blobs), \
        f"{test_gcs_path} not found in GCS Bronze layer."

    # Download and validate the data
    blob = bucket.blob(test_gcs_path)
    local_test_file = "test_reddit_data.parquet"
    blob.download_to_filename(local_test_file)
    
    # Read the parquet file back
    df = pd.read_parquet(local_test_file)
    
    # Data Quality Checks
    # 1. Check that data was collected (non-empty DataFrame)
    assert not df.empty, "No data was collected - DataFrame is empty"
    logging.info(f"✅ Collected {len(df)} Reddit posts/comments")
    
    # 2. Check expected columns exist
    expected_columns = [
        "ticker", "company", "ticker_risk", "name_risk", "subreddit",
        "term_used", "post_id", "created_utc", "title", "text",
        "score", "url", "source"
    ]
    missing_columns = set(expected_columns) - set(df.columns)
    assert len(missing_columns) == 0, \
        f"Missing expected columns: {missing_columns}"
    logging.info("✅ All expected columns present")
    
    # 3. Check data types and non-null values for critical columns
    assert df["ticker"].notna().all(), "Some ticker values are null"
    assert df["post_id"].notna().all(), "Some post_id values are null"
    assert df["created_utc"].notna().all(), "Some created_utc values are null"
    assert df["source"].eq("reddit").all(), "All sources should be 'reddit'"
    logging.info("✅ Critical columns have no null values")
    
    # 4. Check for duplicate posts (should be deduplicated by ticker+post_id)
    duplicates = df.duplicated(subset=["ticker", "post_id"]).sum()
    assert duplicates == 0, f"Found {duplicates} duplicate posts"
    logging.info("✅ No duplicate posts found")
    
    # 5. Validate sample records
    sample_record = df.iloc[0]
    assert isinstance(sample_record["ticker"], str), "Ticker should be string"
    assert isinstance(sample_record["post_id"], str), "Post ID should be string"
    assert isinstance(sample_record["created_utc"], (int, float)), \
        "Created UTC should be numeric timestamp"
    logging.info("✅ Sample record validation passed")
    
    # 6. Check that we have data for the expected number of tickers
    unique_tickers = df["ticker"].nunique()
    assert unique_tickers > 0, "No unique tickers found in data"
    logging.info(f"✅ Data collected for {unique_tickers} unique tickers")
    
    # 7. Check subreddit distribution
    unique_subs = df["subreddit"].nunique()
    assert unique_subs > 0, "No subreddits found in data"
    logging.info(f"✅ Data collected from {unique_subs} subreddits")
    
    # 8. Generate test summary report
    logging.info("\n" + "=" * 70)
    logging.info("TEST SUMMARY REPORT")
    logging.info("=" * 70)
    logging.info(f"Total records collected: {len(df)}")
    logging.info(f"Unique tickers: {df['ticker'].nunique()}")
    logging.info(f"Unique subreddits: {df['subreddit'].nunique()}")
    logging.info(f"Date range: {pd.to_datetime(df['created_utc'], unit='s').min()} to {pd.to_datetime(df['created_utc'], unit='s').max()}")
    logging.info(f"Average score: {df['score'].mean():.2f}")
    logging.info(f"GCS path: gs://{bucket_name}/{test_gcs_path}")
    logging.info("=" * 70)
    
    # Clean up local test file
    if os.path.exists(local_test_file):
        os.remove(local_test_file)
    
    # Confirm execution of Reddit ingestion pipeline.
    logging.info("✅ Full Reddit ingestion pipeline executed successfully with data quality validation.")