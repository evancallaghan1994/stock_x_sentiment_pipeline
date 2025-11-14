# Stock Sentiment ELT Pipeline
*An automated data pipeline combining historical stock data and news analysis using Databricks, Google Cloud, and Airflow.*
<br>

## Overview
This project implements an ELT data pipeline that integrates financial market data with news sentiment analytics. The pipeline ingests daily historical OHLCV stock data for 15 major stocks and ETFs (including AAPL, NVDA, TSLA, MSFT, GOOGL, META, and others) from Yahoo Finance and combines it with news articles and headlines extracted from the Stock News API. Orchestration and workflow automation are managed by Apache Airflow, which coordinates and schedules daily incremental extraction, transformation, and load processes. The primary objective is to engineer a reliable and maintainable ELT pipeline that delivers high-quality, analysis-ready data for downstream analytical and modeling workloads. The curated data is subsequently used to analyze relationships between market sentiment and stock performance, generate and refresh interactive Tableau dashboards, and evaluate whether sentiment dynamics exhibit predictive value for future price movements.

Developed with Python, PySpark, Databricks, Google Cloud, and Apache Airflow, the system follows a Medallion architecture (Bronze → Silver → Gold) to ensure scalable, incremental data processing and lineage clarity. Data ingestion and transformation are implemented through modular Python components and production-ready transformation scripts, while orchestration and automation are executed via Airflow DAGs. News articles are analyzed using FINbert (financial BERT) and VADER sentiment models to generate comprehensive sentiment scores. Processed datasets are persisted in a Google Cloud Storage–based data lake and subsequently loaded into BigQuery for SQL-driven analytics and Tableau dashboard integration. This project delivers a fully automated and scalable ELT pipeline designed with production-level reliability, observability, and reproducibility across ingestion, transformation, storage, and analytics layers.

<br>

## Repository Navigation Directions
<br><br>

## Architecture
<br><br>

## Business Understanding
<br><br>

## Tech Stack

| **Category** | **Tools & Technologies** |
|-----------|----------------------|
| Programming | Python, PySpark |
| Data Platform | Databricks, Google Cloud Storage |
| Orchestration | Apache Airflow |
| Data Warehouse | BigQuery |
| Visualization | Tableau |
| Architecture | Medallion (Bronze &rarr; Silver &rarr; Gold) |
| Version Control & Deployment | Git, Docker |

<br>

## Data Sources & Understanding
<br>

- Yahoo Finance: Stock Data
- Stock News API: News Articles and Headlines

<br>

## Data Flow & Medallion Architecture
<br>

- Bronze Layer: GCS bucket for raw data
- Silver Layer: BigQuery for transformed data
- Gold Layer: 

<br>

## Pipeline Componants
<br>

- fetch_data/ - Data ingestion scripts
- transformation_scripts/ - converted transformation pipelines
- airflow_dags/ - orchestration
- setup_scripts/ - infrastructure setup
- notebooks/ - jupyter notebooks for exploratory transformation

<br>

## Automation
<br><br>

## Data Preparation & Transformation
<br>

- Data cleaning/validation steps
- Feature engineering
- Technical indicators calculated from daily closing price
- Sentiment analysis via FINbert and VADER

<br>

## Testing & Data Quality
<br>

- Validation notebooks
- Pytest tests
- Data quality checks
- Validation reports

<br>

## Exploratory Analysis & Visualization
<br><br>

## Modeling
<br><br>

## Results & Evaluation
<br><br>

## Setup & Installation
<br><br>

## Future Enhancements
<br><br>

## Links & Resources
<br><br>

## Repository Navigation
<br><br>

## Author & Contact
<br><br>

## License
This project is publicly accessible for educational and portfolio demonstration purposes only.  
See the [LICENSE](LICENSE) file for full terms of use.
<br>