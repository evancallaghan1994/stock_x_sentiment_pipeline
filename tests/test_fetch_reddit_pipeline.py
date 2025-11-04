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
    storage_client,
    bucket_name,
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