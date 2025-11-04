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

    # Confirm execution of Reddit ingestion pipeline.
    logging.info("Full Reddit ingestion pipeline executed successfully.")