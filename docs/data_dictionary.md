# Data Dictionary

## Overview
This dataset supports a daily ETL pipeline that collects stock prices and stock-related news, enriches them with technical indicators and sentiment scores, and stores them in BigQuery for analytical and ML use cases.

## Dataset Metadata
**Dataset:** {GCP_PROJECT_ID}.{BIGQUERY_DATASET}
**REGION:** US (multi-region)
**Primary Consumers:** ML Models, dashboards, analytics workloads
**Update Frequency:** Daily

## Tables

### 1. News Sentiment Table

**Table Name:** `{BIGQUERY_TABLE_NEWS}`  
**Description:** News articles with sentiment analysis scores from Stock News API
**Intended Use:** Preprocessed articles with sentiment scores ready for analytics and ML feature engineering in Gold layer
**Partitioning:** Monthly by `date_key`  
**Clustering:** `ticker`, `date_key`  
**Data Source:** Stock News API (via `fetch_data/fetch_sn.py`)

#### Schema

| Column Name | Data Type | Mode | Description |
|------------|-----------|------|-------------|
| `ticker` | STRING | REQUIRED | Stock ticker symbol (e.g., "AAPL", "MSFT") |
| `company_name` | STRING | NULLABLE | Full company name |
| `date_key` | DATE | REQUIRED | Date key for joining with price data (YYYY-MM-DD format). Used as partition key. |
| `article_date` | DATE | NULLABLE | Original article publication date |
| `query_date_parsed` | DATE | NULLABLE | Date the article was queried from the API |
| `title` | STRING | REQUIRED | Article headline/title |
| `text` | STRING | NULLABLE | Full article text content |
| `url` | STRING | NULLABLE | Article URL/source link |
| `search_source` | STRING | NULLABLE | Source of search query used to find article. Values: "ticker", "company_name", "abbrev1", "abbrev2" |
| `endpoint_used` | STRING | NULLABLE | API endpoint used to fetch article. Values: "advanced", "regular" |
| `finbert_score` | FLOAT | NULLABLE | FINbert sentiment score ranging from -1 (negative) to 1 (positive) |
| `finbert_label` | STRING | NULLABLE | FINbert sentiment classification. Values: "positive", "negative", "neutral" |
| `vader_compound` | FLOAT | NULLABLE | VADER compound sentiment score ranging from -1 (negative) to 1 (positive) |
| `vader_pos` | FLOAT | NULLABLE | VADER positive sentiment score ranging from 0 to 1 |
| `vader_neu` | FLOAT | NULLABLE | VADER neutral sentiment score ranging from 0 to 1 |
| `vader_neg` | FLOAT | NULLABLE | VADER negative sentiment score ranging from 0 to 1 |
| `ingestion_timestamp` | TIMESTAMP | NULLABLE | Timestamp when data was ingested into BigQuery |
| `sentiment_timestamp` | TIMESTAMP | NULLABLE | Timestamp when sentiment analysis was performed |
| `data_source` | STRING | NULLABLE | Data source identifier. Value: "stock_news_api" |

#### Notes
- **Primary Key:** `(ticker, date_key, url)` - combination ensures uniqueness
- **Join Key:** `date_key` is used to join with stock prices table
- **Sentiment Models:**
  - **FINbert:** Financial domain-specific BERT model fine-tuned for financial sentiment
  - **VADER:** Valence Aware Dictionary and sEntiment Reasoner - rule-based sentiment analyzer
- Articles are deduplicated based on `(ticker, date_key, url)` before loading

---

### 2. Stock Prices Table

**Table Name:** `{BIGQUERY_TABLE_STOCKS}`  
**Description:** Stock prices with technical indicators and engineered features
**Intended Use:** Preprocessed daily prices for analytics and technical feature engineering in Gold layer
**Partitioning:** Monthly by `date`  
**Clustering:** `ticker`, `date`  
**Data Source:** Yahoo Finance (via `fetch_data/fetch_yfinance_data.py`)

#### Schema

##### Core Price Data

| Column Name | Data Type | Mode | Description |
|------------|-----------|------|-------------|
| `ticker` | STRING | REQUIRED | Stock ticker symbol (e.g., "AAPL", "MSFT") |
| `date` | DATE | REQUIRED | Trading date. Used as partition key. |
| `date_key` | DATE | REQUIRED | Date key for joining with sentiment data (YYYY-MM-DD format). Typically same as `date`. |
| `open` | FLOAT | NULLABLE | Opening price for the trading day |
| `high` | FLOAT | NULLABLE | Highest price during the trading day |
| `low` | FLOAT | NULLABLE | Lowest price during the trading day |
| `close` | FLOAT | REQUIRED | Closing price for the trading day |
| `volume` | FLOAT | NULLABLE | Trading volume (number of shares traded) |

##### Metadata

| Column Name | Data Type | Mode | Description |
|------------|-----------|------|-------------|
| `ingestion_timestamp` | TIMESTAMP | NULLABLE | Timestamp when data was ingested into BigQuery |

#### Notes
- **Primary Key:** `(ticker, date)` - ensures one record per ticker per day
- **Join Key:** `date_key` is used to join with news sentiment table
- **Data Quality:** Invalid OHLC relationships are auto-corrected (high >= low, high >= open/close, low <= open/close)
- **Technical Indicators:** Calculated using standard financial formulas and rolling windows
- **Null Values:** Early dates may have NULL values for indicators requiring historical data (e.g., 50-day SMA requires 50 days of history)

---

## Data Sources

1. **Stock Prices:** Yahoo Finance API (via `yfinance` Python library)
2. **News Articles:** Stock News API (via `fetch_data/fetch_sn.py`)

---

## Update Frequency

- **Stock Prices:** Daily (after market close)
- **News Articles:** Daily (incremental fetch based on last ingestion date)
- **Transformations:** Daily (via Airflow DAG)

---

## Data Quality

- **Deduplication:** Applied at transformation stage
- **Validation:** Bronze layer validation notebooks check for nulls, duplicates, date continuity
- **Auto-fixing:** Invalid OHLC relationships are corrected automatically
- **Incremental Loading:** Only new data since last ingestion is fetched

---

## Notes

- All timestamps are in UTC
- Date fields use DATE type (YYYY-MM-DD format)
- FLOAT types are used for all numeric calculations to maintain precision
- Partitioning and clustering optimize query performance for time-series analysis
- Tables are designed for analytical workloads (OLAP) rather than transactional (OLTP)