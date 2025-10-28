# Stock Sentiment ELT Pipeline
*An automated data pipeline combining historical stock data and Reddit sentiment analysis using Databricks, Google Cloud, and Airflow.*
<br><br>

## Overview
This project implements an ELT data pipeline that integrates financial market data with social sentiment analytics. The pipeline ingests five years of daily historical OHLCV stock data for all S&P 500 companies from Yahoo Finance and combines it with Reddit post and comment data extracted from the ten leading subreddits focused on stocks and investing. Orchestration and workflow automation are managed by Apache Airflow, which coordinates and schedules daily extraction, transformation, and load processes. The primary objective is to engineer a reliable and maintainable ELT pipeline that delivers high-quality, analysis-ready data for downstream analytical and modeling workloads. The curated data is subsequently used to analyze relationships between market sentiment and stock performance, generate and refresh interactive Tableau dashboards, and evaluate whether sentiment dynamics exhibit predictive value for future price movements. 

Developed with Python, PySpark, Databricks, Google Cloud, and Apache Airflow, the system follows a Medallion architecture (Bronze &rarr; Silver &rarr; Gold) to ensure scalable, incremental data processing and lineage clarity. Data ingestion and transformation are implemented through modular Python components and Databricks notebooks, while orchestration and automation are executed via Airflow DAGs. Processed datasets are persisted in a Google Cloud Storage–based data lake and subsequently loaded into a data warehouse (BigQuery or Databricks SQL) for SQL-driven analytics and Tableau dashboard integration. This project delivers a fully automated and scalable ELT pipeline designed with production-level reliability, observability, and reproducibility across ingestion, transformation, storage, and analytics layers.
<br><br><br>

## Repository Navigation Directions
<br><br><br>

## Architecture
<br><br><br>

## Business Understanding
<br><br><br>

## Tech Stack
<br><br><br>

## Data Sources & Understanding
<br><br><br>

## Data Flow & Medallion Architecture
<br><br><br>

## Pipeline Componants
<br><br><br>

## Automation
<br><br><br>

## Data Preparation & Transformation
<br><br><br>

## Testing & Data Quality
<br><br><br>

## Exploratory Analysis & Visualization
<br><br><br>

## Modeling
<br><br><br>

## Results & Evaluation
<br><br><br>

## Setup & Installation
<br><br><br>

## Future Enhancements
<br><br><br>

## Links & Resources
<br><br><br>

## Repository Navigation
<br><br><br>

## Author & Contact
<br><br><br>


## License
This project is publicly accessible for educational and portfolio demonstration purposes only.  
See the [LICENSE](LICENSE) file for full terms of use.
<br><br><br>