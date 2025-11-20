# Stock Sentiment ELT Pipeline
*An automated data pipeline combining historical stock data and news analysis using Google Cloud and Airflow.*
<br>

## Overview
This project implements an ELT data pipeline that integrates financial market data with news sentiment analytics. The pipeline ingests daily historical OHLCV stock data for 15 major stocks and ETFs (including AAPL, NVDA, TSLA, MSFT, GOOGL, META, and others) from Yahoo Finance and combines it with news articles and headlines extracted from the Stock News API. Orchestration and workflow automation are managed by Apache Airflow, which coordinates and schedules daily incremental extraction, transformation, and load processes. The primary objective is to engineer a reliable and maintainable ELT pipeline that delivers high-quality, analysis-ready data for downstream analytical and modeling workloads. The curated data is subsequently used to analyze relationships between market sentiment and stock performance, generate and refresh interactive Tableau dashboards, and evaluate whether sentiment dynamics exhibit predictive value for future price movements.

Developed with Python, PySpark, Google Cloud, and Apache Airflow, the system follows a Medallion architecture (Bronze → Silver → Gold) to ensure scalable, incremental data processing and lineage clarity. Data ingestion and transformation are implemented through modular Python components and production-ready transformation scripts, while orchestration and automation are executed via Airflow DAGs. News articles are analyzed using FINbert (financial BERT) and VADER sentiment models to generate comprehensive sentiment scores. Processed datasets are persisted in a Google Cloud Storage–based data lake and subsequently loaded into BigQuery for SQL-driven analytics and Tableau dashboard integration. This project delivers a fully automated and scalable ELT pipeline designed with production-level reliability, observability, and reproducibility across ingestion, transformation, storage, and analytics layers.
<br>

## Repository Navigation Directions

<br>

**Key Directories:**
- `fetch_data/` - Data ingestion scripts for Yahoo Finance and Stock News API
- `transformation_scripts/` - Production-ready Python scripts for data transformation
- `airflow_dags/` - Airflow DAGs for pipeline orchestration
- `notebooks/` - Jupyter notebooks for exploratory analysis and validation
- `setup_scripts/` - BigQuery table setup and infrastructure scripts
- `tests/` - Unit and integration tests
- `config/` - Configuration files (paths, metadata)
- `docs/` - Documentation (data dictionary, etc.)
<br>

## Architecture
<br>
The pipeline follows a **Medallion Architecture** pattern with three distinct layers:

1. **Bronze Layer (Raw Data)**: Google Cloud Storage (GCS) bucket storing raw, unprocessed data from APIs
   - Stock prices stored as Parquet files per ticker
   - News articles stored as Parquet files with metadata

2. **Silver Layer (Cleaned & Transformed)**: BigQuery data warehouse containing:
   - Transformed stock prices with 60+ technical indicators
   - News articles with sentiment analysis scores (FINbert + VADER)
   - Partitioned and clustered for optimal query performance

3. **Gold Layer (Curated)**: Aggregated datasets for specific use cases
   - Daily sentiment-price joins for Tableau dashboards
   - ML-ready feature sets for predictive modeling
   - Pre-computed analytics and aggregations

**Pipeline Flow:**
Data Sources &rarr; Airflow DAG &rarr; Bronze (GCS) &rarr; Validation &rarr; Transformation &rarr; Silver (BigQuery) &rarr; Gold (Analytics)

<br>

## Business Understanding

<br>

The pipeline enables analysis of relationships between financial news sentiment and stock price movements. By combining technical indicators with sentiment scores, analysts can:
- Identify correlations between news sentiment and price changes
- Build predictive models for price movements
- Create real-time dashboards monitoring market sentiment
- Perform time-series analysis on sentiment trends

<br>

## Tech Stack

| **Category** | **Tools & Technologies** |
|-----------|----------------------|
| Programming | Python, PySpark |
| Data Platform | Google Cloud Storage |
| Orchestration | Apache Airflow |
| Data Warehouse | Google BigQuery |
| Visualization | Tableau |
| Architecture | Medallion (Bronze &rarr; Silver &rarr; Gold) |
| Version Control & Deployment | Git, Docker |
| Testing | Pytest, Pytest-asyncio |

<br>

## Data Sources & Understanding
<br>

- Yahoo Finance: Stock Data
- Stock News API: News Articles and Headlines

<br>

## Data Flow & Medallion Architecture
<br>

**Bronze Layer (GCS):**
- Raw stock prices
- Raw news articles & headlines

**Silver Layer:**
- Cleaned and transformed data stored in BigQuery
- Data is processed from Bronze via transformation scripts, then loaded into BigQuery tables
- **Stock Prices Table:**
    - Cleaned and transformed
    - Feature-engineered metrics (technical analysis indicators, support/resistance, etc.)
- **News Sentiment Table:**
    - Cleaned and transformed
    - Sentiment analysis via FINbert and VADER

- **Gold Layer (Pending):**
    - Daily aggregated sentiment-price joins for Tableau
    - ML feature sets for price prediction models
    - Pre-computed analytics and time-series aggregations

<br>

## Pipeline Components

<br>

**Data Ingestion (`fetch_data/`):**
- `fetch_yfinance_data.py` - Fetches stock OHLCV data from Yahoo Finance
- `fetch_sn.py` - Fetches news articles from Stock News API
- Both support incremental fetching based on last ingestion date

**Transformation (`transformation_scripts/`):**
- `transform_raw_stocks.py` - Transforms stock prices, calculates 60+ technical indicators
- `news_sentiment.py` - Transforms news data, runs FINbert and VADER sentiment analysis

**Orchestration (`airflow_dags/`):**
- `market_pipeline_dag.py` - Main DAG orchestrating the entire pipeline
- `incremental_fetch.py` - Wrapper functions for incremental data fetching
- `utils.py` - Utility functions for date range calculation from BigQuery

**Validation (`notebooks/`):**
- `validate_prices.ipynb` - Validates stock price data quality
- `validate_news.ipynb` - Validates news article data quality

**Infrastructure (`setup_scripts/`):**
- `setup_bigquery.py` - Creates BigQuery tables for news sentiment
- `setup_bigquery_stocks.py` - Creates BigQuery tables for stock prices

<br>

## Automation

<br>

The pipeline is fully automated via **Apache Airflow** with the `market_pipeline` DAG:

**DAG Tasks:**
1. `get_date_ranges` - Calculates incremental date ranges from BigQuery
2. `ingest_prices` - Fetches new stock prices since last run
3. `ingest_news` - Fetches new news articles since last run
4. `validate_bronze` - Validates raw data quality (placeholder)
5. `transform_prices` - Transforms prices and loads to BigQuery
6. `transform_news` - Transforms news and loads to BigQuery
7. `load_bigquery` - No-op (transformations handle loading)
8. `notify_success` - Completion notification

**Key Features:**
- **Incremental Processing**: Only fetches data since last successful run
- **Error Handling**: Automatic retries with exponential backoff
- **Dependency Management**: Tasks run in proper sequence with dependencies
- **Manual Trigger**: Currently set to manual trigger (`schedule=None`), can be changed to `@daily`

**Running the Pipeline:**
- Start Airflow scheduler
```bash
airflow scheduler
```

- Start Airflow API server (in separate terminal)
```bash
airflow api-server --port 8080
```

- Trigger DAG via UI or CLI
    - airflow dags trigger market_pipeline
    - The pipeline is fully automated via **Apache Airflow** with the `market_pipeline` DAG:

**DAG Tasks:**
1. `get_date_ranges` - Calculates incremental date ranges from BigQuery
2. `ingest_prices` - Fetches new stock prices since last run
3. `ingest_news` - Fetches new news articles since last run
4. `validate_bronze` - Validates raw data quality (placeholder)
5. `transform_prices` - Transforms prices and loads to BigQuery
6. `transform_news` - Transforms news and loads to BigQuery
7. `load_bigquery` - No-op (transformations handle loading)
8. `notify_success` - Completion notification

**Key Features:**
- **Incremental Processing**: Only fetches data since last successful run
- **Error Handling**: Automatic retries with exponential backoff
- **Dependency Management**: Tasks run in proper sequence with dependencies
- **Manual Trigger**: Currently set to manual trigger (`schedule=None`), can be changed to `@daily`

<br>

## Data Preparation & Transformation

<br>

**Stock Price Transformation:**
- Data cleaning: Removes duplicates, fixes invalid OHLC relationships
- Feature engineering: 60+ technical indicators including:
  - Moving averages (SMA, EMA): 5, 10, 20, 50 day periods
  - Momentum indicators: RSI, MACD, price momentum
  - Volatility metrics: Bollinger Bands, rolling volatility
  - Volume analysis: VWAP, volume ratios
  - Support/Resistance levels: 20-day and 50-day
  - Candlestick patterns: Doji, Hammer, Engulfing patterns
- Data validation: Ensures price relationships (high >= low, etc.)

**News Sentiment Transformation:**
- Data cleaning: Deduplication based on (ticker, date_key, url)
- Text preprocessing: Combines title and text for sentiment analysis
- Sentiment analysis:
  - **FINbert**: Financial domain-specific BERT model (scores -1 to 1)
  - **VADER**: Rule-based sentiment analyzer (compound, pos, neu, neg scores)
- Feature extraction: Date parsing, source identification, metadata preservation

**Data Quality:**
- Automatic deduplication
- Null value handling
- Date continuity checks
- Schema validation before BigQuery load

<br>

## Testing & Data Quality

<br>

**Unit Tests (`tests/`):**
- `test_fetch_yfinance_data.py` - Tests stock data fetching
- `test_fetch_sn.py` - Tests news data fetching
- Pytest configuration with async support

**Validation Notebooks (`notebooks/`):**
- `validate_prices.ipynb`:
  - Checks for null dates, duplicates, date continuity
  - Validates OHLC relationships, price ranges
  - Auto-fixes invalid data where possible
  
- `validate_news.ipynb`:
  - Checks for null dates, required fields
  - Identifies duplicate articles
  - Validates date ranges and data completeness

**Data Quality Checks:**
- Bronze layer validation before transformation
- Schema validation before BigQuery writes
- Incremental data validation (only new data checked)

**Running Tests:**
```bash
pytest tests/ -v
```
- Run specific test file
```bash
pytest tests/test_fetch_yfinance_data.py -v
```

<br>

## Exploratory Analysis & Visualization
<br><br>

## Modeling
<br><br>

## Results & Evaluation
<br><br>

## Setup & Installation

<br>

**Prerequisites:**
- Python 3.11+, Java 17+, Google Cloud account, Stock News API key, Airflow 2.4+

**Quick Start:**

1. **Clone and setup:**
```bash
git clone <repository-url>
```
```bash
cd stock_x_sentiment
```
```bash
python3.11 -m venv
```
```bash
source venv/bin/activate
```
or Windows: 
```bash
venv\Scripts\activate
```
```bash
pip install -r requirements.txt
```
2. **Configure environment:**
```bash
cp .env.example .env
```
- Edit .env with your GCP credentials, BigQuery settings, and API keys
3. **Initialize infrastructure:**
```bash
python setup_scripts/setup_bigquery.py
```
```bash
python setup_scripts/setup_bigquery_stocks.py
```
```bash
airflow db migrate
```
```bash
airflow standalone
```
4. **Access Airflow UI:** 
- `http://localhost:8080` (credentials printed in terminal) and trigger `market_pipeline` DAG

**Note:** 
- See `.env.example` for required environment variables. 
- For Docker setup, see `Dockerfile`.

<br>

## Future Enhancements
<br>

- **Gold Layer Implementation**: Create aggregated datasets for Tableau and ML
- **Real-time Processing**: Stream processing for near-real-time sentiment analysis
- **Additional Data Sources**: Social media sentiment (Twitter, Reddit)
- **Advanced ML Models**: Price prediction models using sentiment + technical indicators
- **Automated Alerts**: Email/Slack notifications for pipeline failures
- **Data Quality Monitoring**: Automated data quality dashboards
- **Cost Optimization**: Implement data retention policies and partitioning strategies
- **Production Deployment**: Deploy to cloud (GCP Composer for Airflow)

<br>

## Links & Resources
<br>

- [Data Dictionary](docs/data_dictionary.md) - Complete BigQuery schema documentation
- [Airflow DAGs Documentation](airflow_dags/README.md) - DAG structure and usage
- [Project Configuration](config/project_paths.yaml) - Data path configuration

**External Resources:**
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Google BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [FINbert Model](https://huggingface.co/ProsusAI/finbert)

<br>

## Repository Navigation
<br>

**Quick Start:**
1. Review `README.md` (this file) for overview
2. Check `docs/data_dictionary.md` for data schema
3. Review `airflow_dags/market_pipeline_dag.py` for pipeline structure
4. Run `setup_scripts/` to initialize BigQuery tables
5. Trigger DAG via Airflow UI

**Key Files:**
- `requirements.txt` - Python dependencies
- `.env.example` - Environment variable template
- `Dockerfile` - Container configuration
- `pytest.ini` - Test configuration

<br>

## Author & Contact
<br><br>

## License
This project is publicly accessible for educational and portfolio demonstration purposes only.  
See the [LICENSE](LICENSE) file for full terms of use.
<br>