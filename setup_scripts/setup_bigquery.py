"""
setup_bigquery.py - Set up BigQuery dataset and table for news sentiment data
"""

import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, ".env")
load_dotenv(dotenv_path=env_path)

BIGQUERY_PROJECT = os.getenv("GCP_PROJECT_ID")  # Using your existing variable
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET",)
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE_NEWS",)

if not BIGQUERY_PROJECT:
    raise ValueError("GCP_PROJECT_ID not found in .env file")

print("=" * 70)
print("BIGQUERY SETUP")
print("=" * 70)
print(f"Project ID: {BIGQUERY_PROJECT}")
print(f"Dataset: {BIGQUERY_DATASET}")
print(f"Table: {BIGQUERY_TABLE}")
print("=" * 70)
print()

# Initialize BigQuery client
client = bigquery.Client(project=BIGQUERY_PROJECT)

# Create Dataset
print("Step 1: Creating dataset...")
dataset_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"

try:
    dataset = client.get_dataset(dataset_id)
    print(f"✅ Dataset '{BIGQUERY_DATASET}' already exists")
except NotFound:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset.description = "Stock sentiment analysis data warehouse"
    dataset.labels = {"project": "stock_sentiment", "environment": "production"}
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"✅ Created dataset '{BIGQUERY_DATASET}' in location '{dataset.location}'")

# Create Table Schema
print("\nStep 2: Creating table schema...")
table_id = f"{dataset_id}.{BIGQUERY_TABLE}"

schema = [
    bigquery.SchemaField("ticker", "STRING", mode="REQUIRED", description="Stock ticker symbol"),
    bigquery.SchemaField("company_name", "STRING", mode="NULLABLE", description="Company name"),
    bigquery.SchemaField("date_key", "DATE", mode="REQUIRED", description="Date key for joining with price data (YYYY-MM-DD)"),
    bigquery.SchemaField("article_date", "DATE", mode="NULLABLE", description="Original article publication date"),
    bigquery.SchemaField("query_date_parsed", "DATE", mode="NULLABLE", description="Date the article was queried"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED", description="Article headline/title"),
    bigquery.SchemaField("text", "STRING", mode="NULLABLE", description="Full article text"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE", description="Article URL"),
    bigquery.SchemaField("search_source", "STRING", mode="NULLABLE", description="Source of search (ticker, company_name, abbrev1, abbrev2)"),
    bigquery.SchemaField("endpoint_used", "STRING", mode="NULLABLE", description="API endpoint used (advanced, regular)"),
    bigquery.SchemaField("finbert_score", "FLOAT", mode="NULLABLE", description="FINbert sentiment score (-1 to 1)"),
    bigquery.SchemaField("finbert_label", "STRING", mode="NULLABLE", description="FINbert sentiment label (positive, negative, neutral)"),
    bigquery.SchemaField("vader_compound", "FLOAT", mode="NULLABLE", description="VADER compound sentiment score (-1 to 1)"),
    bigquery.SchemaField("vader_pos", "FLOAT", mode="NULLABLE", description="VADER positive sentiment score (0 to 1)"),
    bigquery.SchemaField("vader_neu", "FLOAT", mode="NULLABLE", description="VADER neutral sentiment score (0 to 1)"),
    bigquery.SchemaField("vader_neg", "FLOAT", mode="NULLABLE", description="VADER negative sentiment score (0 to 1)"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="NULLABLE", description="Timestamp when data was ingested"),
    bigquery.SchemaField("sentiment_timestamp", "TIMESTAMP", mode="NULLABLE", description="Timestamp when sentiment analysis was performed"),
    bigquery.SchemaField("data_source", "STRING", mode="NULLABLE", description="Data source (stock_news_api)"),
]

try:
    table = client.get_table(table_id)
    print(f"✅ Table '{BIGQUERY_TABLE}' already exists")
    print(f"   Current row count: {table.num_rows:,}")
except NotFound:
    table = bigquery.Table(table_id, schema=schema)
    table.description = "News articles with sentiment analysis scores"
    table.labels = {"project": "stock_sentiment", "data_type": "news_sentiment", "environment": "production"}
    table.time_partitioning = bigquery.TimePartitioning(field="date_key", type_=bigquery.TimePartitioningType.MONTH)
    table.clustering_fields = ["ticker", "date_key"]
    table = client.create_table(table)
    print(f"✅ Created table '{BIGQUERY_TABLE}'")
    print(f"   Partitioned by: date_key (monthly)")
    print(f"   Clustered by: ticker, date_key")

# Verify Setup
print("\n" + "=" * 70)
print("SETUP VERIFICATION")
print("=" * 70)
dataset = client.get_dataset(dataset_id)
print(f"✅ Dataset: {dataset_id}")
print(f"   Location: {dataset.location}")

table = client.get_table(table_id)
print(f"\n✅ Table: {table_id}")
print(f"   Rows: {table.num_rows:,}")
print(f"   Partitioned: {table.time_partitioning is not None}")
if table.clustering_fields:
    print(f"   Clustered: {', '.join(table.clustering_fields)}")

print("\n" + "=" * 70)
print("✅ BIGQUERY SETUP COMPLETE")
print("=" * 70)