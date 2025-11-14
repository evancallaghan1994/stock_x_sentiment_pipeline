#!/usr/bin/env python
"""
News Sentiment Analysis Pipeline
Transforms raw news data from GCS Bronze layer, runs sentiment analysis, and loads to BigQuery Silver layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, min as spark_min, max as spark_max,
        when, isnan, isnull, trim, lower, upper, regexp_replace,
        to_date, date_format, lit, concat_ws, split, explode,
        avg, stddev, collect_list, struct, udf
    )
from pyspark.sql.types import *

from pyspark.sql.window import Window

import pandas as pd

from datetime import datetime, timedelta

import os

from dotenv import load_dotenv

from google.cloud import storage, bigquery

from google.cloud.exceptions import NotFound


# Sentiment analysis imports (will be used later)

from transformers import AutoTokenizer, AutoModelForSequenceClassification

import torch

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from tqdm import tqdm


def main():
    """Main function to run the transformation pipeline"""
    # Load environment variables
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    project_root = os.path.dirname(script_dir) if '__file__' in globals() else os.path.dirname(os.getcwd())
    env_path = os.path.join(project_root, ".env")
    load_dotenv(dotenv_path=env_path)

    GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    BIGQUERY_PROJECT = os.getenv("GCP_PROJECT_ID")
    BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
    BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")

    if not GCS_BUCKET_NAME:
        raise ValueError("GCS_BUCKET_NAME not found in .env file")
    if not BIGQUERY_PROJECT:
        raise ValueError("GCP_PROJECT_ID not found in .env file")
    if not BIGQUERY_DATASET:
        raise ValueError("BIGQUERY_DATASET not found in .env file")
    if not BIGQUERY_TABLE:
        raise ValueError("BIGQUERY_TABLE not found in .env file")

    # Resolve credentials path if it's relative
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and not os.path.isabs(credentials_path):
        credentials_path = os.path.join(project_root, credentials_path)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    # Set JAVA_HOME to Java 17 for Spark
    import subprocess
    try:
        java_home = subprocess.check_output(['/usr/libexec/java_home', '-v', '17'], text=True).strip()
        os.environ['JAVA_HOME'] = java_home
        os.environ['PATH'] = f"{java_home}/bin:{os.environ.get('PATH', '')}"
        print(f"✅ JAVA_HOME set to: {java_home}")
    except Exception as e:
        print(f"⚠️  Warning: Could not set JAVA_HOME automatically: {e}")
        print("   Make sure Java 17+ is installed and JAVA_HOME is set manually")

    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("NewsSentimentPipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Initialize BigQuery client
    bq_client = bigquery.Client(project=BIGQUERY_PROJECT)

    # Initialize GCS client
    storage_client = storage.Client(project=BIGQUERY_PROJECT)

    print("=" * 70)
    print("NEWS SENTIMENT ANALYSIS PIPELINE")
    print("=" * 70)
    print(f"GCS Bucket: {GCS_BUCKET_NAME}")
    print(f"BigQuery Project: {BIGQUERY_PROJECT}")
    print(f"BigQuery Dataset: {BIGQUERY_DATASET}")
    print(f"BigQuery Table: {BIGQUERY_TABLE}")
    print("=" * 70)


    # In[5]:


    # ======================================================================
    # CELL 2: Load Data from GCS Bronze Layer
    # ======================================================================
    from google.cloud import storage
    import numpy as np
    import tempfile
    import shutil

    BRONZE_NEWS_PATH = "bronze/news/stock_news_api"

    print("=" * 70)
    print("LOADING DATA FROM GCS")
    print("=" * 70)
    print(f"Bucket: {GCS_BUCKET_NAME}")
    print(f"Path: {BRONZE_NEWS_PATH}")
    print()

    # Initialize GCS client
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    # List all parquet files
    print("Finding parquet files...")
    blobs = bucket.list_blobs(prefix=BRONZE_NEWS_PATH)
    parquet_files = [blob.name for blob in blobs if blob.name.endswith('.parquet')]

    print(f"Found {len(parquet_files)} parquet files")

    # Create a temporary directory to store downloaded files
    temp_dir = tempfile.mkdtemp()
    temp_files = []
    dfs = []

    try:
        # Download all files first
        print("Downloading files from GCS...")
        for file_path in parquet_files:
            try:
                blob = bucket.blob(file_path)
                temp_file_path = os.path.join(temp_dir, os.path.basename(file_path))
                blob.download_to_filename(temp_file_path)
                temp_files.append(temp_file_path)
            except Exception as e:
                print(f"⚠️  Error downloading {file_path}: {e}")
                continue

        if not temp_files:
            raise ValueError("No parquet files were successfully downloaded")

        print(f"Downloaded {len(temp_files)} files")
        print("Loading files with Pandas (to handle timestamp types)...")

        # Load files with Pandas first (handles nanosecond timestamps better)
        for temp_file in temp_files:
            try:
                df_temp = pd.read_parquet(temp_file)
                dfs.append(df_temp)
            except Exception as e:
                print(f"⚠️  Error loading {temp_file}: {e}")
                continue

        if not dfs:
            raise ValueError("No parquet files were successfully loaded")

        # Combine all DataFrames
        print("Combining DataFrames...")
        pandas_df = pd.concat(dfs, ignore_index=True)

        # Convert numpy arrays/lists to Python lists for Spark compatibility
        print("Converting data types for Spark compatibility...")
        for col_name in pandas_df.columns:
            if pandas_df[col_name].dtype == 'object':
                # Check if column contains arrays/lists
                sample = pandas_df[col_name].dropna().iloc[0] if len(pandas_df[col_name].dropna()) > 0 else None
                if sample is not None and isinstance(sample, (list, np.ndarray)):
                    # Convert numpy arrays to Python lists
                    pandas_df[col_name] = pandas_df[col_name].apply(
                        lambda x: x.tolist() if isinstance(x, np.ndarray) else (list(x) if isinstance(x, (list, tuple)) else x)
                    )

        # Drop columns that aren't needed for the pipeline (like 'topics' if it exists)
        columns_to_drop = ['topics']  # Add any other columns that cause issues
        columns_to_drop = [col for col in columns_to_drop if col in pandas_df.columns]
        if columns_to_drop:
            print(f"Dropping columns not needed for pipeline: {columns_to_drop}")
            pandas_df = pandas_df.drop(columns=columns_to_drop)

        # Convert to Spark DataFrame
        print("Converting to Spark DataFrame...")
        news_df = spark.createDataFrame(pandas_df)

        # Force evaluation and cache to ensure data is loaded in memory before cleanup
        print("Caching DataFrame in memory...")
        news_df = news_df.cache()
        record_count = news_df.count()

        print(f"✅ Loaded {record_count:,} articles")
        print()
        print("Schema:")
        news_df.printSchema()
        print()
        print("Sample data:")
        news_df.show(5, truncate=50)

    finally:
        # Clean up temporary directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\n✅ Cleaned up temporary files")


    # In[6]:


    # ======================================================================
    # CELL 3: Data Validation (Production-Level)
    # ======================================================================
    print("=" * 70)
    print("DATA VALIDATION")
    print("=" * 70)

    # 1. Schema Validation
    required_columns = ['ticker', 'query_date', 'title']
    missing_columns = [col for col in required_columns if col not in news_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # 2. Data Quality Checks
    validation_results = {}

    validation_results['null_ticker'] = news_df.filter(col('ticker').isNull()).count()
    validation_results['null_date'] = news_df.filter(col('query_date').isNull()).count()
    validation_results['null_title'] = news_df.filter(col('title').isNull()).count()
    validation_results['empty_title'] = news_df.filter(
        (col('title').isNull()) | (trim(col('title')) == '')
    ).count()

    # Check date range validity (last 2 years to future dates)
    current_date = datetime.now().strftime('%Y-%m-%d')
    two_years_ago = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    validation_results['invalid_dates'] = news_df.filter(
        (col('query_date') < two_years_ago) | (col('query_date') > current_date)
    ).count()

    # Check for duplicate articles
    if 'url' in news_df.columns:
        validation_results['duplicate_urls'] = news_df.groupBy('url').count().filter(col('count') > 1).count()
    else:
        validation_results['duplicate_title_date'] = news_df.groupBy('title', 'query_date').count().filter(col('count') > 1).count()

    # Print validation results
    print("Validation Results:")
    for check, result in validation_results.items():
        status = "✅ PASS" if result == 0 else f"⚠️  {result} issues found"
        print(f"  {check}: {status}")

    # Filter out invalid records
    valid_df = news_df.filter(
        col('ticker').isNotNull() &
        col('query_date').isNotNull() &
        col('title').isNotNull() &
        (trim(col('title')) != '')
    )

    print(f"\nValid records: {valid_df.count():,} / {news_df.count():,}")


    # In[7]:


    # ======================================================================
    # CELL 4: Data Transformation and Cleaning
    # ======================================================================
    print("=" * 70)
    print("DATA TRANSFORMATION")
    print("=" * 70)

    # 1. Standardize date columns
    transformed_df = valid_df.withColumn(
        'article_date',
        to_date(col('date'), 'yyyy-MM-dd')
    ).withColumn(
        'query_date_parsed',
        to_date(col('query_date'), 'yyyy-MM-dd')
    ).withColumn(
        'date_key',  # For joining with price data later
        date_format(col('query_date_parsed'), 'yyyy-MM-dd')
    )

    # 2. Clean text fields
    transformed_df = transformed_df.withColumn(
        'title_clean',
        trim(lower(regexp_replace(col('title'), r'[^\w\s]', '')))
    ).withColumn(
        'text_clean',
        when(col('text').isNotNull(),
             trim(lower(regexp_replace(col('text'), r'[^\w\s]', ''))))
        .otherwise(lit(''))
    )

    # 3. Extract text for sentiment analysis (title + text if available)
    transformed_df = transformed_df.withColumn(
        'sentiment_text',
        when(col('text').isNotNull() & (trim(col('text')) != ''),
             concat_ws(' ', col('title'), col('text')))
        .otherwise(col('title'))
    )

    # 4. Ensure ticker is uppercase
    transformed_df = transformed_df.withColumn(
        'ticker',
        upper(col('ticker'))
    )

    # 5. Add metadata columns
    transformed_df = transformed_df.withColumn(
        'ingestion_timestamp',
        lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    ).withColumn(
        'data_source',
        lit('stock_news_api')
    )

    # 6. Select and order columns for final schema
    final_columns = [
        'ticker',
        'company_name',
        'date_key',
        'article_date',
        'query_date_parsed',
        'title',
        'text',
        'sentiment_text',
        'url',
        'search_source',
        'endpoint_used',
        'ingestion_timestamp',
        'data_source'
    ]

    # Add any additional columns that exist
    for col_name in final_columns:
        if col_name not in transformed_df.columns:
            transformed_df = transformed_df.withColumn(col_name, lit(None))

    transformed_df = transformed_df.select(*final_columns)

    print(f"✅ Transformed {transformed_df.count():,} articles")
    print("Sample data:")
    transformed_df.show(5, truncate=50)


    # In[8]:


    # ======================================================================
    # CELL 5: Prepare Data for Sentiment Analysis
    # ======================================================================
    print("=" * 70)
    print("PREPARING DATA FOR SENTIMENT ANALYSIS")
    print("=" * 70)

    # Convert Spark DataFrame to Pandas for sentiment analysis
    # For very large datasets, consider using Spark UDFs or distributed processing
    print("Converting Spark DataFrame to Pandas for sentiment analysis...")
    sentiment_df = transformed_df.select(
        'ticker', 'date_key', 'sentiment_text', 'title', 'url'
    ).toPandas()

    print(f"Prepared {len(sentiment_df):,} articles for sentiment analysis")
    print(f"\nSample text length statistics:")
    print(sentiment_df['sentiment_text'].str.len().describe())
    print(f"\nSample sentiment text:")
    print(sentiment_df['sentiment_text'].head(3).tolist())


    # In[5]:


    # ======================================================================
    # CELL 4: Initialize Sentiment Analysis Models
    # ======================================================================
    print("=" * 70)
    print("INITIALIZING SENTIMENT MODELS")
    print("=" * 70)

    # Initialize FINbert with safetensors to avoid torch.load vulnerability
    print("Loading FINbert model (this may take a few minutes on first run)...")
    try:
        finbert_tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
        finbert_model = AutoModelForSequenceClassification.from_pretrained(
            "yiyanghkust/finbert-tone",
            use_safetensors=True  # Use safetensors format to avoid torch.load issue
        )
        finbert_model.eval()
    except Exception as e:
        print(f"Error loading with safetensors, trying alternative method: {e}")
        # Fallback: try with trust_remote_code
        finbert_tokenizer = AutoTokenizer.from_pretrained("yiyanghkust/finbert-tone")
        finbert_model = AutoModelForSequenceClassification.from_pretrained(
            "yiyanghkust/finbert-tone",
            trust_remote_code=True
        )
        finbert_model.eval()

    # Initialize VADER
    print("Loading VADER analyzer...")
    vader_analyzer = SentimentIntensityAnalyzer()

    print("✅ Models loaded successfully")


    # In[6]:


    print(f"Total articles to process: {len(sentiment_df):,}")


    # In[7]:


    # ======================================================================
    # CELL 5: Run Sentiment Analysis
    # ======================================================================
    print("=" * 70)
    print("RUNNING SENTIMENT ANALYSIS")
    print("=" * 70)

    def get_finbert_sentiment(text, tokenizer, model, max_length=512):
        """Get FINbert sentiment score"""
        if pd.isna(text) or text == '':
            return None, None

        text = str(text)[:max_length]
        inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=max_length)

        with torch.no_grad():
            outputs = model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        labels = ['positive', 'negative', 'neutral']
        scores = predictions[0].tolist()
        dominant_idx = scores.index(max(scores))
        dominant_sentiment = labels[dominant_idx]
        compound_score = scores[0] - scores[1]  # positive - negative

        return compound_score, dominant_sentiment

    def get_vader_sentiment(text, analyzer):
        """Get VADER sentiment scores"""
        if pd.isna(text) or text == '':
            return None, None, None, None

        scores = analyzer.polarity_scores(str(text))
        return scores['compound'], scores['pos'], scores['neu'], scores['neg']

    # Enable progress bar for pandas apply
    tqdm.pandas(desc="Processing articles")

    # Apply sentiment analysis with progress bar
    print("Running FINbert analysis (this will take ~2 hours)...")
    print("Progress:")
    finbert_results = sentiment_df['sentiment_text'].progress_apply(
        lambda x: pd.Series(get_finbert_sentiment(x, finbert_tokenizer, finbert_model))
    )
    sentiment_df[['finbert_score', 'finbert_label']] = finbert_results

    print("\nRunning VADER analysis (this will take ~6 minutes)...")
    print("Progress:")
    vader_results = sentiment_df['sentiment_text'].progress_apply(
        lambda x: pd.Series(get_vader_sentiment(x, vader_analyzer))
    )
    sentiment_df[['vader_compound', 'vader_pos', 'vader_neu', 'vader_neg']] = vader_results

    sentiment_df['sentiment_timestamp'] = datetime.now()

    print(f"\n✅ Completed sentiment analysis on {len(sentiment_df):,} articles")
    print("\nSentiment distribution (FINbert):")
    print(sentiment_df['finbert_label'].value_counts())
    print("\nSentiment distribution percentages:")
    print(sentiment_df['finbert_label'].value_counts(normalize=True) * 100)


    # In[ ]:


    # ======================================================================
    # CELL 8: Merge Sentiment Scores Back to Spark DataFrame
    # ======================================================================
    print("=" * 70)
    print("MERGING SENTIMENT SCORES")
    print("=" * 70)

    # Convert sentiment results back to Spark DataFrame
    sentiment_spark_df = spark.createDataFrame(sentiment_df)

    # Join with transformed data
    final_df = transformed_df.join(
        sentiment_spark_df.select(
            'ticker', 'date_key', 'url',
            'finbert_score', 'finbert_label',
            'vader_compound', 'vader_pos', 'vader_neu', 'vader_neg',
            'sentiment_timestamp'
        ),
        on=['ticker', 'date_key', 'url'],
        how='left'
    )

    print(f"✅ Final dataset: {final_df.count():,} articles with sentiment scores")
    print("\nSample data:")
    final_df.select('ticker', 'date_key', 'title', 'finbert_score', 'finbert_label', 'vader_compound').show(10, truncate=50)


    # In[ ]:


    # ======================================================================
    # CELL 9: Duplicate Check and Summary Statistics
    # ======================================================================
    print("=" * 70)
    print("DUPLICATE CHECK AND SUMMARY")
    print("=" * 70)

    print(f"Transformed df record count: {transformed_df.count():,}")
    print(f"Sentiment df record count: {len(sentiment_df):,}")
    print(f"Final df record count: {final_df.count():,}")

    # Check for duplicate keys in final DataFrame
    print(f"\nDuplicate check on merge keys (ticker, date_key, url):")
    duplicate_count = final_df.groupBy('ticker', 'date_key', 'url').count().filter(col('count') > 1).count()
    print(f"Duplicate combinations: {duplicate_count}")

    if duplicate_count > 0:
        print(f"\n⚠️  WARNING: {duplicate_count} duplicate (ticker, date_key, url) combinations found!")
        duplicates = final_df.groupBy('ticker', 'date_key', 'url').count().filter(col('count') > 1)
        print("Sample duplicates:")
        duplicates.show(10)

    # Check unique combinations
    print(f"\nUnique combinations:")
    unique_count = final_df.select('ticker', 'date_key', 'url').distinct().count()
    print(f"Unique (ticker, date_key, url) combinations: {unique_count:,}")

    # Check articles per stock
    print("\n" + "=" * 70)
    print("ARTICLES PER STOCK")
    print("=" * 70)
    articles_per_stock = final_df.groupBy('ticker').count().orderBy(col('count').desc())
    articles_per_stock.show(truncate=False)

    total_articles = final_df.count()
    unique_tickers = final_df.select('ticker').distinct().count()
    print(f"\nTotal articles: {total_articles:,}")
    print(f"Average articles per stock: {total_articles / unique_tickers:.1f}")
    print(f"Number of unique stocks: {unique_tickers}")


    # In[ ]:


    # ======================================================================
    # CELL 10: Write to BigQuery
    # ======================================================================
    print("=" * 70)
    print("WRITING TO BIGQUERY")
    print("=" * 70)

    # Convert Spark DataFrame to Pandas for BigQuery write
    print("Converting Spark DataFrame to Pandas...")
    final_pandas_df = final_df.toPandas()

    # Drop intermediate columns that aren't in BigQuery schema
    columns_to_drop = ['sentiment_text', 'title_clean', 'text_clean']  # Add any other intermediate columns
    columns_to_drop = [col for col in columns_to_drop if col in final_pandas_df.columns]
    if columns_to_drop:
        final_pandas_df = final_pandas_df.drop(columns=columns_to_drop)
        print(f"Dropped intermediate columns: {columns_to_drop}")

    # Convert date columns to proper format for BigQuery
    if 'date_key' in final_pandas_df.columns:
        final_pandas_df['date_key'] = pd.to_datetime(final_pandas_df['date_key']).dt.date
    if 'article_date' in final_pandas_df.columns:
        final_pandas_df['article_date'] = pd.to_datetime(final_pandas_df['article_date']).dt.date
    if 'query_date_parsed' in final_pandas_df.columns:
        final_pandas_df['query_date_parsed'] = pd.to_datetime(final_pandas_df['query_date_parsed']).dt.date

    # Convert timestamp columns
    if 'ingestion_timestamp' in final_pandas_df.columns:
        final_pandas_df['ingestion_timestamp'] = pd.to_datetime(final_pandas_df['ingestion_timestamp'])
    if 'sentiment_timestamp' in final_pandas_df.columns:
        final_pandas_df['sentiment_timestamp'] = pd.to_datetime(final_pandas_df['sentiment_timestamp'])

    # Write to BigQuery
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    print(f"\nWriting {len(final_pandas_df):,} records to BigQuery...")
    print(f"Table: {table_id}")
    print(f"Columns being written: {list(final_pandas_df.columns)}")

    job = bq_client.load_table_from_dataframe(
        final_pandas_df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER
        )
    )
    job.result()  # Wait for the job to complete

    print(f"✅ Successfully wrote {len(final_pandas_df):,} records to BigQuery")
    print(f"Table: {table_id}")

    # Verify write
    table = bq_client.get_table(table_id)
    print(f"\n✅ Verification:")
    print(f"   Table rows: {table.num_rows:,}")
    print(f"   Table size: {table.num_bytes / (1024*1024):.2f} MB")


    # In[9]:


    # ======================================================================
    # CELL 8: Validation and Summary
    # ======================================================================
    print("=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)

    # Query BigQuery to verify
    query = f"""
    SELECT
        COUNT(*) as total_articles,
        COUNT(DISTINCT ticker) as unique_tickers,
        COUNT(DISTINCT date_key) as unique_dates,
        AVG(finbert_score) as avg_finbert_score,
        AVG(vader_compound) as avg_vader_compound,
        COUNT(CASE WHEN finbert_label = 'positive' THEN 1 END) as positive_count,
        COUNT(CASE WHEN finbert_label = 'negative' THEN 1 END) as negative_count,
        COUNT(CASE WHEN finbert_label = 'neutral' THEN 1 END) as neutral_count
    FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
    """

    results = bq_client.query(query).to_dataframe()
    print(results.to_string())

    print("\n✅ Pipeline completed successfully!")
    print("=" * 70)



if __name__ == "__main__":
    main()
