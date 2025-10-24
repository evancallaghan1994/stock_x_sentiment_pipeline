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

