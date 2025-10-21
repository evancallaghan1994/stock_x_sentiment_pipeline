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
# -------------------------------------------------------------------------
# Add Imports
# -------------------------------------------------------------------------
import os # For environment variables and file paths
import time # Time management to avoid rate limiting
import pandas as pd # Data manipulation and analysis
from dotenv import load_dotenv # For loading environment variables
from wordfreq import word_frequency # For getting word frequency
import praw # For Reddit API
import re # Regular expressions for text processing

# -------------------------------------------------------------------------
# Step 0: Load environment variables + Reddit API credentials
# -------------------------------------------------------------------------
load_dotenv()

# Create a Reddit API client
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent=os.getenv("REDDIT_USER_AGENT")
)
# Restrict code to read-only moode; avoid accidental writing to Reddit
reddit.read_only = True

# -------------------------------------------------------------------------
# Step 1: Word Frequency Analysis
# -------------------------------------------------------------------------
stock_ticker_name_df = pd.read_csv("sp500_tickers.csv")

# Function to calculate risk frequency for 
# a given stock ticker/company name
def classify_wordfreq(term: str):
    """Return low/medium/high based on English frequency."""
    freq = word_frequency(term.lower(), "en")
    if freq > 1e-5:
        risk = "high"
    elif freq > 1e-6:
        risk = "medium"
    else:
        risk = "low"
    return risk, freq

# Loop through each ticker/company name and calculate risk frequency
wordfreq_risk_scores = []
for _, row in stock_ticker_name_df.iterrows():
    symbol = row["Symbol"] # Ticker symbol
    full_company_name = str(row["Security"]) # Company name
    # Company names can be multiple words and sometimes it's
    # necessary. If the company name is more than one word, 
    # we will use two words to calculate the risk, unless the 
    # second word is corporate suffix like 'inc' or 'corp'.
    # Need to keep ampersand (&) for special cases (PG&E, AT&T)
    corporate_suffixes = {
        "inc", "incorporated", "corp", "corporation", 
        "ltd", "llc"
    }

    # Some companies have parentheses in the name that must be removed
    name_no_paren = re.sub(r"\(.*?\)", "", full_company_name).strip()

    name_parts = [
        re.sub(r"[^\w&]", "", word) 
        for word in name_no_paren.split()
    ]
    name_filtered = [
        word for word in name_parts 
        if word.lower() not in corporate_suffixes
    ]

    # For company names that are more than one word after filtering
    # recombine into a single string
    if len(name_filtered) > 1:
        company_name = (
            " ".join(name_filtered[:2])
        )
    else:
        company_name = (
            name_filtered[0] 
            if name_filtered 
            else name_no_paren
        )

    # Calculate risk frequency for company name
    ticker_risk, ticker_freq = (
        classify_wordfreq(symbol
    )
    company_name_risk, company_name_freq = (
        classify_wordfreq(company_name)
    )

    wordfreq_risk_scores.append(
        {
            "Symbol": symbol,
            "CompanyName": company_name,
            "TickerRisk_wordfreq": ticker_risk,
            "TickerFreq": ticker_freq,
            "NameRisk_wordfreq": company_name_risk,
            "NameFreq": company_name_freq,
        }
    )
wordfreq_df = pd.DataFrame(wordfreq_risk_scores)

# -------------------------------------------------------------------------
# Step 2: Reddit Noise Risk Analysis
    # Check top 10 highest traffic subreddits unrelated to stocks/investing
    # to see if any of the low/medium risk words appear frequently enough 
    # to warrant a higher risk score. 
# -------------------------------------------------------------------------
popular_subreddits = [
    "funny", "AskReddit", "gaming", "worldnews",
    "todayilearned", "aww", "music", "memes",
    "pics", "videos"
]

reddit_cache = {}

def classify_reddit_noise(term: str):
    """ Search through subreddits and classify based on number of hits"""
    if term in reddit_cache:
        return reddit_cache[term]

    total_hits = 0
    for sub in popular_subreddits:
        try:
            results = reddit.subreddit(sub).search(term, limit=25)
            total_hits += len(results)
        except Exception as e:
            print(f"Error searching subreddit {sub}: {e}")
        time.sleep(0.8)

    if total_hits > 10:
        risk = "high"
    elif total_hits >= 5:
        risk = "medium"
    else:
        risk = "low"

    reddit_cache[term] = (risk, total_hits)
    return reddit_cache[term]   
    
