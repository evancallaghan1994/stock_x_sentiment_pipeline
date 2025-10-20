"""
This script builds a metadata file for our list of 
S&P 500 tickers estimating how risky each ticker/company name
is for Reddit searches. Some stock tickers (OR, ON, AI) and 
company names (Apple, Alphabet, Google, Shell) are non-unique 
english words that are used frequently outside the context of 
the stock market. In order to reduce the risk of our sentiment
analysis being contaminated by these non-unique words, we will
assign a risk score to each ticker/company name based on 
frequency of the word in the English language. We will do this
in a 3 step process:
    1. Use WordFreq Python package to get the frequency of each word 
    in the English language and assign it to a risk bucket based on 
    frequency of use. The higher the frequency, the higher the risk.
    2. Using the top 10-20 highest traffic reddit subreddits, we
    will comb through to see if any of the low/medium risk words appear
    frequently enough to warrant a higher risk score.
    3. Manually review the results and adjust the risk scores as needed.
"""

import os # For environment variables and file paths
import time # Time management to avoid rate limiting
import pandas as pd # Data manipulation and analysis
from dotenv import load_dotenv # For loading environment variables
from wordfreq import word_frequency # For getting word frequency
import praw # For Reddit API

# Step 0: Load environment variables + Reddit API credentials
load_dotenv()

# Create a Reddit API client
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)
# Restrict code to read-only moode; avoid accidental writing to Reddit
reddit.read_only = True