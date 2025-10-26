"""
This script searches for mentions of S&P 500 stock tickers
and company names in the stock-related subreddits stored 
in reddit_subs.yaml. The results are stored in a pandas 
DataFrame and uploaded to a GCP bucket. 
"""

#-------------------------------------------------------------------------
# Imports
#-------------------------------------------------------------------------
import os # For environment variables and file paths
import time # Time management to avoid rate limiting
import pandas as pd # Data manipulation and analysis
from dotenv import load_dotenv # For loading environment variables
import praw # For Reddit API
import re # Regular expressions for text processing
import yaml # Reading and parsing YAML files
from tqdm import tqdm # Displays progress bar
from datetime import datetime # For working with dates and times
from google.cloud import storage # For interacting with Google Cloud

#-------------------------------------------------------------------------
# Load environment variables
#-------------------------------------------------------------------------
load_dotenv() # Load environment variables from .env file

# Initialize Reddit and GCP clients
# Reddit
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)
# Restrict code to read-only mode; avoid accidental writing to Reddit
reddit.read_only = True

# GCP
bucket_name = os.getenv("GCS_BUCKET_NAME")
storage_client = storage.Client()

# Load config files (investing subreddits + sp500 metadata risk scores)
subs = yaml.safe_load(open("config/reddit_subs.yaml"))["subs"]
sp500_metadata = pd.read_csv("config/sp500_metadata.csv")

#-------------------------------------------------------------------------
# Reddit crawler
# Using one search term at a time, we crawl through the subreddits,
    # filter out irrelevant posts, and return a list of dictionaries
#-------------------------------------------------------------------------
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
            print(f"⚠️ Error in subreddit {sub}, term {term}: {e}")
            time.sleep(2)

    return collected

#-------------------------------------------------------------------------
# Search logic
# $TICKER is always used to search first. It should be enough to find posts
    # and is low-risk.
# If $TICKER results in too few posts, we allow fallback to the ticker symbol itself.
#-------------------------------------------------------------------------