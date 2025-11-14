"""
setup_bigquery_stocks.py - Set up BigQuery dataset and table for stock prices data
"""

import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, ".env")
load_dotenv(dotenv_path=env_path)

BIGQUERY_PROJECT = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE_STOCKS")

if not BIGQUERY_PROJECT:
    raise ValueError("GCP_PROJECT_ID not found in .env file")
if not BIGQUERY_DATASET:
    raise ValueError("BIGQUERY_DATASET not found in .env file")
if not BIGQUERY_TABLE:
    raise ValueError("BIGQUERY_TABLE_STOCKS not found in .env file")

print("=" * 70)
print("BIGQUERY SETUP - STOCK PRICES")
print("=" * 70)
print(f"Project ID: {BIGQUERY_PROJECT}")
print(f"Dataset: {BIGQUERY_DATASET}")
print(f"Table: {BIGQUERY_TABLE}")
print("=" * 70)
print()

# Initialize BigQuery client
client = bigquery.Client(project=BIGQUERY_PROJECT)

# Create Dataset (if it doesn't exist)
print("Step 1: Checking dataset...")
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
    bigquery.SchemaField("date", "DATE", mode="REQUIRED", description="Trading date"),
    bigquery.SchemaField("date_key", "DATE", mode="REQUIRED", description="Date key for joining with sentiment data (YYYY-MM-DD)"),
    bigquery.SchemaField("open", "FLOAT", mode="NULLABLE", description="Opening price"),
    bigquery.SchemaField("high", "FLOAT", mode="NULLABLE", description="High price"),
    bigquery.SchemaField("low", "FLOAT", mode="NULLABLE", description="Low price"),
    bigquery.SchemaField("close", "FLOAT", mode="REQUIRED", description="Closing price"),
    bigquery.SchemaField("volume", "FLOAT", mode="NULLABLE", description="Trading volume"),
    bigquery.SchemaField("prev_close", "FLOAT", mode="NULLABLE", description="Previous day's closing price"),
    bigquery.SchemaField("price_change", "FLOAT", mode="NULLABLE", description="Daily price change (close - prev_close)"),
    bigquery.SchemaField("price_change_pct", "FLOAT", mode="NULLABLE", description="Daily price change percentage"),
    bigquery.SchemaField("sma_5", "FLOAT", mode="NULLABLE", description="5-day Simple Moving Average"),
    bigquery.SchemaField("sma_10", "FLOAT", mode="NULLABLE", description="10-day Simple Moving Average"),
    bigquery.SchemaField("sma_20", "FLOAT", mode="NULLABLE", description="20-day Simple Moving Average"),
    bigquery.SchemaField("sma_50", "FLOAT", mode="NULLABLE", description="50-day Simple Moving Average"),
    bigquery.SchemaField("ema_5", "FLOAT", mode="NULLABLE", description="5-day Exponential Moving Average"),
    bigquery.SchemaField("ema_10", "FLOAT", mode="NULLABLE", description="10-day Exponential Moving Average"),
    bigquery.SchemaField("ema_20", "FLOAT", mode="NULLABLE", description="20-day Exponential Moving Average"),
    bigquery.SchemaField("ema_50", "FLOAT", mode="NULLABLE", description="50-day Exponential Moving Average"),
    bigquery.SchemaField("daily_range", "FLOAT", mode="NULLABLE", description="Daily price range (high - low)"),
    bigquery.SchemaField("daily_range_pct", "FLOAT", mode="NULLABLE", description="Daily range as percentage of close"),
    bigquery.SchemaField("volatility_5d", "FLOAT", mode="NULLABLE", description="5-day rolling volatility (std dev of returns)"),
    bigquery.SchemaField("volatility_10d", "FLOAT", mode="NULLABLE", description="10-day rolling volatility"),
    bigquery.SchemaField("volatility_20d", "FLOAT", mode="NULLABLE", description="20-day rolling volatility"),
    bigquery.SchemaField("volume_sma_5", "FLOAT", mode="NULLABLE", description="5-day volume moving average"),
    bigquery.SchemaField("volume_sma_10", "FLOAT", mode="NULLABLE", description="10-day volume moving average"),
    bigquery.SchemaField("volume_sma_20", "FLOAT", mode="NULLABLE", description="20-day volume moving average"),
    bigquery.SchemaField("volume_ratio", "FLOAT", mode="NULLABLE", description="Current volume / 20-day average volume"),
    bigquery.SchemaField("vwap_cumulative", "FLOAT", mode="NULLABLE", description="Cumulative Volume Weighted Average Price"),
    bigquery.SchemaField("vwap_20d", "FLOAT", mode="NULLABLE", description="20-day rolling VWAP"),
    bigquery.SchemaField("rsi_14", "FLOAT", mode="NULLABLE", description="14-day Relative Strength Index (0-100)"),
    bigquery.SchemaField("momentum_5d", "FLOAT", mode="NULLABLE", description="5-day momentum (price change)"),
    bigquery.SchemaField("momentum_5d_pct", "FLOAT", mode="NULLABLE", description="5-day momentum percentage"),
    bigquery.SchemaField("momentum_20d", "FLOAT", mode="NULLABLE", description="20-day momentum (price change)"),
    bigquery.SchemaField("momentum_20d_pct", "FLOAT", mode="NULLABLE", description="20-day momentum percentage"),
    bigquery.SchemaField("bb_middle", "FLOAT", mode="NULLABLE", description="Bollinger Band middle (20-day SMA)"),
    bigquery.SchemaField("bb_upper", "FLOAT", mode="NULLABLE", description="Bollinger Band upper"),
    bigquery.SchemaField("bb_lower", "FLOAT", mode="NULLABLE", description="Bollinger Band lower"),
    bigquery.SchemaField("bb_width", "FLOAT", mode="NULLABLE", description="Bollinger Band width"),
    bigquery.SchemaField("bb_percent_b", "FLOAT", mode="NULLABLE", description="Bollinger Band %B (0-1)"),
    bigquery.SchemaField("macd", "FLOAT", mode="NULLABLE", description="MACD line (12-day EMA - 26-day EMA)"),
    bigquery.SchemaField("macd_signal", "FLOAT", mode="NULLABLE", description="MACD signal line"),
    bigquery.SchemaField("macd_histogram", "FLOAT", mode="NULLABLE", description="MACD histogram"),
    bigquery.SchemaField("log_return", "FLOAT", mode="NULLABLE", description="Logarithmic daily return"),
    bigquery.SchemaField("cumulative_return", "FLOAT", mode="NULLABLE", description="Cumulative log return"),
    bigquery.SchemaField("sharpe_ratio_20d", "FLOAT", mode="NULLABLE", description="20-day rolling Sharpe ratio"),
    bigquery.SchemaField("sharpe_ratio_60d", "FLOAT", mode="NULLABLE", description="60-day rolling Sharpe ratio"),
    bigquery.SchemaField("drawdown", "FLOAT", mode="NULLABLE", description="Current drawdown from peak"),
    bigquery.SchemaField("drawdown_pct", "FLOAT", mode="NULLABLE", description="Drawdown percentage"),
    bigquery.SchemaField("is_doji", "INTEGER", mode="NULLABLE", description="Doji candlestick pattern (0 or 1)"),
    bigquery.SchemaField("is_hammer", "INTEGER", mode="NULLABLE", description="Hammer candlestick pattern (0 or 1)"),
    bigquery.SchemaField("is_bullish_engulfing", "INTEGER", mode="NULLABLE", description="Bullish engulfing pattern (0 or 1)"),
    bigquery.SchemaField("is_bearish_engulfing", "INTEGER", mode="NULLABLE", description="Bearish engulfing pattern (0 or 1)"),
    bigquery.SchemaField("price_position", "FLOAT", mode="NULLABLE", description="Close price position in daily range (0-1)"),
    bigquery.SchemaField("support_20d", "FLOAT", mode="NULLABLE", description="20-day support level (rolling minimum low)"),
    bigquery.SchemaField("resistance_20d", "FLOAT", mode="NULLABLE", description="20-day resistance level (rolling maximum high)"),
    bigquery.SchemaField("distance_to_support_20d", "FLOAT", mode="NULLABLE", description="Percentage distance to 20-day support"),
    bigquery.SchemaField("distance_to_resistance_20d", "FLOAT", mode="NULLABLE", description="Percentage distance to 20-day resistance"),
    bigquery.SchemaField("support_50d", "FLOAT", mode="NULLABLE", description="50-day support level"),
    bigquery.SchemaField("resistance_50d", "FLOAT", mode="NULLABLE", description="50-day resistance level"),
    bigquery.SchemaField("distance_to_support_50d", "FLOAT", mode="NULLABLE", description="Percentage distance to 50-day support"),
    bigquery.SchemaField("distance_to_resistance_50d", "FLOAT", mode="NULLABLE", description="Percentage distance to 50-day resistance"),
    bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="NULLABLE", description="Timestamp when data was ingested"),
]

try:
    table = client.get_table(table_id)
    print(f"✅ Table '{BIGQUERY_TABLE}' already exists")
    print(f"   Current row count: {table.num_rows:,}")
except NotFound:
    table = bigquery.Table(table_id, schema=schema)
    table.description = "Stock prices with technical indicators and features"
    table.labels = {"project": "stock_sentiment", "data_type": "stock_prices", "environment": "production"}
    table.time_partitioning = bigquery.TimePartitioning(field="date", type_=bigquery.TimePartitioningType.MONTH)
    table.clustering_fields = ["ticker", "date"]
    table = client.create_table(table)
    print(f"✅ Created table '{BIGQUERY_TABLE}'")
    print(f"   Partitioned by: date (monthly)")
    print(f"   Clustered by: ticker, date")

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

