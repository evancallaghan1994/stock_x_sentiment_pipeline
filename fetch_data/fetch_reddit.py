"""
fetch_reddit.py
────────────────────────────────────────────────────────────────────────
Author: Evan Callaghan
Created: 2025-10-25
Description: 
    This script searches for mentions of S&P 500 stock tickers
    and company names in the stock-related subreddits stored 
    in reddit_subs.yaml. The results are stored as a pandas 
    DataFrame and uploaded to a GCP bucket. 

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
import time
from datetime import datetime
import re
import logging

# ----------------------------------------------------------------------
# Third-party Imports
# ----------------------------------------------------------------------
import pandas as pd # Data manipulation and analysis
from dotenv import load_dotenv # For loading environment variables
import praw # For Reddit API
import yaml # Reading and parsing YAML files
from tqdm import tqdm # Displays progress bar
from google.cloud import storage # For interacting with Google Cloud


# ======================================================================
# Logging Configuration
# ======================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ======================================================================
# Environment Setup & Configuration
# ======================================================================
load_dotenv() # Load environment variables from .env file

# ----------------------------------------------------------------------
# Initialize Reddit and GCP clients
# ----------------------------------------------------------------------
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)
reddit.read_only = True # Prevents accidental writing to Reddit

bucket_name = os.getenv("GCS_BUCKET_NAME")
storage_client = storage.Client()

# ----------------------------------------------------------------------
# Load Config Files (investing subreddits + sp500 metadata risk scores)
# ----------------------------------------------------------------------
subs = yaml.safe_load(open("config/reddit_subs.yaml"))["subs"]
asset_meta = pd.read_csv("config/sp500_metadata.csv")
logging.info(
    f"Loaded {len(subs)} subreddits and {len(asset_meta)} assets from metadata."
)


# ======================================================================
# Reddit Crawler Function
# ======================================================================
# Using one search term at a time, this function crawls through the subreddits,
# filters out irrelevant posts, and returns a list of dictionaries.
# ----------------------------------------------------------------------
def search_reddit_for_term(term, row, cutoff, per_term_limit):
  
    collected = []
    for sub in subs:
        try:
            for post in reddit.subreddit(sub).search(term, sort="new", limit=per_term_limit):
                # stop early if post is older than our lookback window
                if post.created_utc < cutoff:
                    break

                # combine title + body text
                text = (post.title or "") + " " + (post.selftext or "")

                # sanity check: make sure ticker symbol actually appears
                if not re.search(rf"\b\$?{row['Symbol']}\b", text, re.IGNORECASE):
                    continue

                collected.append(
                    {
                        "ticker": row["Symbol"],
                        "company": row["CompanyName"],
                        "ticker_risk": row["TickerRisk_total"],
                        "name_risk": row["NameRisk_total"],
                        "subreddit": sub,
                        "term_used": term,
                        "post_id": post.id,
                        "created_utc": post.created_utc,
                        "title": post.title,
                        "text": text,
                        "score": post.score,
                        "url": post.url,
                        "source": "reddit",
                    }
                )
            # brief delay per subreddit to respect API limits
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error in subreddit {sub}, term {term}: {e}")
            time.sleep(2)

    return collected


# ======================================================================
# Search Logic (Term Selection + Fallback Policy)
# ======================================================================
# $TICKER is always used to search first. It should be enough to find posts
# and is low-risk. If $TICKER results in too few posts, we allow fallback 
# to the ticker symbol itself. This function determines which search term 
# to use, then uses the Reddit crawler function to crawl reddit and pull 
# posts with that term. 
# ----------------------------------------------------------------------
def collect_for_ticker(row, lookback_hours=48, per_term_limit=50, min_posts_for_fallback=10):
    
    """
    Collect recent posts for a single ticker.
    Precision-first strategy:
      1. Always search $TICKER (high precision)
      2. Add plain TICKER only if low risk
      3. If too few posts (< min_posts_for_fallback) and medium/high risk → fallback (allow plain TICKER)
      4. Add "CompanyName" only if low-risk and ticker not high-risk
    """

    cutoff = time.time() - lookback_hours * 3600
    rows = []

    # 1. Always include $TICKER (finance-specific usage)
    base_terms = [f"${row['Symbol']}"]

    # 2. Include plain TICKER if low risk
    if row["TickerRisk_total"] == "low":
        base_terms.append(row["Symbol"])

    # Collect posts for all base (safe) terms
    for term in base_terms:
        rows += search_reddit_for_term(term, row, cutoff, per_term_limit)

    # 3. Fallback: if too few posts AND ticker is medium/high risk
    if len(rows) < min_posts_for_fallback and row["TickerRisk_total"] in ("medium", "high"):
        logging.warning(
            f"Enabling fallback for {row['Symbol']} "
            f"(only {len(rows)} posts found from high-precision terms)"
            )
        rows += search_reddit_for_term(row["Symbol"], row, cutoff, per_term_limit)

    # 4. Include safe company name if allowed
    if (
        isinstance(row["CompanyName"], str)
        and row["NameRisk_total"] == "low"
        and row["TickerRisk_total"] != "high"
    ):
        rows += search_reddit_for_term(f"\"{row['CompanyName']}\"", row, cutoff, per_term_limit)

    return rows

# ======================================================================
# Reddit Ingestion Pipeline Orchestration 
# ======================================================================
# Loads S&P 500 tickers and company names from S&P 500 metadata file, 
# instantiates collect_for_ticker() function to crawl subreddit posts
# for each ticker/company name and store as list of dictionaries, 
# converts the list of dictionaries into a pandas DataFrame, 
# drops duplicate posts (within each individual ticker/name), 
# and saves as parquet file and uploads to GCP bucket (bronze layer).
# ----------------------------------------------------------------------
def run_reddit_ingestion(sample_n=40):
    logging.info("Starting Reddit ingestion pipeline...")

    sample = asset_meta.sample(n=min(sample_n, len(asset_meta)), random_state=42)
    all_rows = []

    for _, row in tqdm(sample.iterrows(), total=len(sample), desc="Fetching Reddit posts"):
        all_rows += collect_for_ticker(row)

    df = pd.DataFrame(all_rows).drop_duplicates(["ticker", "post_id"])
    logging.info(f"Collected {len(df)} Reddit posts.")

    if df.empty:
        logging.warning("No posts collected. Check filters or rate limits.")
        return

    # Save locally
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    local_file = f"reddit_raw_{date_str}.parquet"
    df.to_parquet(local_file, index=False)

    # Upload to GCS Bronze
    blob_path = f"bronze/reddit/ingest_date={date_str}/reddit_raw.parquet"
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_filename(local_file)
    logging.info(f"Uploaded to gs://{bucket_name}/{blob_path}")
    logging.info("Reddit ingestion pipeline completed successfully.")

# ======================================================================
# Script Entry Point
# ======================================================================
if __name__ == "__main__":
    run_reddit_ingestion()
