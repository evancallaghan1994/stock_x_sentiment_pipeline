# Reddit Ingestion Pipeline - Testing Guide

## Overview

This guide explains how to test the Reddit ingestion pipeline with production-level practices.

## Pre-Flight Checklist

**Before running tests, verify all prerequisites:**

```bash
# Run the pre-flight check script
python tests/preflight_check.py
```

This will verify:
- ✅ Required config files exist (`config/sp500_metadata.csv`, `config/reddit_subs.yaml`)
- ✅ Environment variables are set (Reddit API credentials, GCS bucket name)
- ✅ Python dependencies are installed
- ✅ Reddit API authentication works
- ✅ GCS credentials are valid

## Running the Test

### 1. Run the Integration Test

```bash
# Run with verbose output to see all checks
pytest tests/test_fetch_reddit_pipeline.py -v

# Run with detailed logging
pytest tests/test_fetch_reddit_pipeline.py -v -s

# Run and show print statements
pytest tests/test_fetch_reddit_pipeline.py -v -s --log-cli-level=INFO
```

### 2. What the Test Does

The test (`test_fetch_reddit_pipeline.py`) performs:

1. **Pipeline Execution**: Runs `run_reddit_ingestion()` with a small sample (3 tickers)
2. **GCS Upload Verification**: Confirms file was uploaded to Bronze layer
3. **Data Download**: Downloads the parquet file from GCS
4. **Data Quality Checks**:
   - Non-empty DataFrame
   - All expected columns present
   - No null values in critical columns
   - No duplicate posts
   - Valid data types
   - Data collected for expected tickers
   - Data from multiple subreddits
5. **Summary Report**: Generates a detailed test summary

### 3. Verifying Test Success

**Immediate Verification (from test output):**

When the test passes, you'll see:
```
✅ Collected X Reddit posts/comments
✅ All expected columns present
✅ Critical columns have no null values
✅ No duplicate posts found
✅ Sample record validation passed
✅ Data collected for X unique tickers
✅ Data collected from X subreddits

======================================================================
TEST SUMMARY REPORT
======================================================================
Total records collected: X
Unique tickers: X
Unique subreddits: X
Date range: YYYY-MM-DD to YYYY-MM-DD
Average score: X.XX
GCS path: gs://bucket-name/bronze/reddit/ingest_date=YYYY-MM-DD/reddit_raw.parquet
======================================================================

✅ Full Reddit ingestion pipeline executed successfully with data quality validation.
```

**If the test fails:**
- Review the error message
- Check the pre-flight checklist items
- Verify GCS permissions and Reddit API limits

## Production-Level Verification

### Option 1: Databricks Validation (Recommended)

For production environments, validate the data in Databricks after ingestion:

1. **Open Databricks Notebook**: `databricks_notebooks/validate_reddit_data.py`
2. **Update the ingestion date** in the script to match your test run
3. **Run the validation script** - it performs comprehensive checks:
   - Schema validation
   - Record counts
   - Null value checks
   - Duplicate detection
   - Data distribution analysis
   - Date range validation
   - Score distribution
   - Risk distribution

This provides:
- ✅ End-to-end validation (GCS → Databricks)
- ✅ Spark-based validation (matches production environment)
- ✅ Detailed quality metrics
- ✅ Ready for downstream processing verification

### Option 2: Manual GCS Inspection

You can also manually inspect the data:

```python
from google.cloud import storage
import pandas as pd

# Download and inspect
client = storage.Client()
bucket = client.bucket("your-bucket-name")
blob = bucket.blob("bronze/reddit/ingest_date=YYYY-MM-DD/reddit_raw.parquet")
blob.download_to_filename("local_check.parquet")

df = pd.read_parquet("local_check.parquet")
print(df.head())
print(df.info())
print(df.describe())
```

## Test Configuration

### Sample Size

The test uses a small sample (3 tickers) by default to:
- Minimize API calls during testing
- Reduce test execution time
- Keep test data manageable

To change the sample size, modify the fixture in `test_fetch_reddit_pipeline.py`:

```python
@pytest.fixture(scope="module")
def sample_size():
    return 3  # Change this number
```

### Test Data Location

Test data is uploaded to the same GCS bucket as production, but with today's date:
```
gs://{bucket}/bronze/reddit/ingest_date={YYYY-MM-DD}/reddit_raw.parquet
```

This ensures the test validates the exact same path structure as production.

## Troubleshooting

### Common Issues

1. **"Missing sp500_metadata.csv"**
   - Run: `python fetch_data/sp500_metadata.py` to generate the file

2. **"Reddit API authentication failed"**
   - Verify `.env` file has correct credentials
   - Test with: `python tests/test_praw_auth.py`

3. **"GCS credentials error"**
   - Ensure `GOOGLE_APPLICATION_CREDENTIALS` is set
   - Or verify service account key in `gcp_keys/`

4. **"No data collected"**
   - Check Reddit API rate limits
   - Verify subreddits in `config/reddit_subs.yaml` are accessible
   - Check if sample tickers have recent posts

## Next Steps After Successful Test

1. ✅ Test passes → Data is validated and ready
2. ✅ Run Databricks validation → Verify end-to-end pipeline
3. ✅ Proceed to sentiment analysis → Data is production-ready

## Production Deployment

When deploying to production:

1. **Increase sample size** in `run_reddit_ingestion()` (default is 40)
2. **Schedule regular runs** via Airflow DAG
3. **Monitor data quality** using Databricks validation script
4. **Set up alerts** for test failures or data quality issues

