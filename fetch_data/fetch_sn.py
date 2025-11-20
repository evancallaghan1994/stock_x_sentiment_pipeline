"""
fetch_sn.py - Stock News API Data Collection Script
────────────────────────────────────────────────────────────────────────
Author: Evan Callaghan
Description: 
    This script uses Stock News API (stocknewsapi.com) to collect news 
    articles for all 25 stocks/ETFs from stock_metadata.csv.
    Collects 20 articles per day from 1 year ago until today.
    Results are stored as a pandas DataFrame and uploaded to GCP Bronze layer.
    
    Strategy:
    1. Try Advanced endpoint (tickers-only) first for precision
    2. Fallback to Regular endpoint (tickers) if < 15 articles
    3. Use items=50 with sortby=rank to get top articles
    4. Paginate if still < 20 articles (fetch additional pages)
    
    Premium plan: 50,000 calls/month, historical data back to March 2019
"""

import os
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm
from google.cloud import storage
import json
import logging

# Load environment variables
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
env_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path=env_path)

SN_API_KEY = os.getenv("SN_API_KEY")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

if not SN_API_KEY:
    raise ValueError("SN_API_KEY not found in .env file")
if not GCS_BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME not found in .env file")

# Configuration
TARGET_ARTICLES_PER_DAY = 10  # or 12
LOOKBACK_DAYS = 365
API_DELAY = 0.25  # 0.25s = 4 calls/sec (under 5/sec limit)
ITEMS_PER_PAGE = 50  # Fetch 50 items per page (buffer to select top 20)
FALLBACK_THRESHOLD = 7  # or 8 (half of target, so if < 7, try fallbacks)
MAX_PAGES = 10  # Max pages to fetch per day (shouldn't need more than 2-3)
PROGRESS_FILE = os.path.join(project_root, "sn_news_progress.json")

# ======================================================================
# Logging Configuration
# ======================================================================
logs_dir = os.path.join(project_root, "logs")
os.makedirs(logs_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, "sn_news.log")),
        logging.StreamHandler()  # Also print to console
    ]
)

# Initialize GCS client (lazy initialization)
_storage_client = None

def get_storage_client():
    """Get or create GCS storage client"""
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client()
    return _storage_client

# Load stock metadata
config_path = os.path.join(project_root, "config", "stock_metadata.csv")
stock_df = pd.read_csv(config_path)
tickers = stock_df['Symbol'].tolist()

def log_collection_start():
    """Log collection start information"""
    logging.info("=" * 70)
    logging.info("STOCK NEWS API - DATA COLLECTION")
    logging.info("=" * 70)
    logging.info(f"Total stocks/ETFs: {len(tickers)}")
    logging.info(f"Target articles per day: {TARGET_ARTICLES_PER_DAY}")
    logging.info(f"Lookback period: {LOOKBACK_DAYS} days")
    logging.info(f"Estimated total API calls: {len(tickers) * LOOKBACK_DAYS}")
    logging.info(f"Premium plan limit: 50,000 calls/month")
    logging.info("=" * 70)
    logging.info("")


# Load progress
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, 'r') as f:
        progress = json.load(f)
else:
    progress = {
        'completed_tickers': [],
        'current_ticker': None,
        'current_ticker_progress': {}
    }

def fetch_articles_for_day(search_term, date, endpoint_type='advanced', page=1):
    """
    Fetch articles for a specific search term (ticker or company name) and date.
    
    Args:
        search_term: Stock ticker symbol or company name
        date: datetime object for the date
        endpoint_type: 'advanced' (tickers-only) or 'regular' (tickers)
        page: Page number to fetch
    
    Returns:
        list: List of article dictionaries, or None if error
    """
    date_str = date.strftime('%m%d%Y')  # MMDDYYYY format
    
    # Build URL based on endpoint type
    if endpoint_type == 'advanced':
        url = (
            f"https://stocknewsapi.com/api/v1?"
            f"tickers-only={search_term}&"
            f"items={ITEMS_PER_PAGE}&"
            f"date={date_str}-{date_str}&"
            f"page={page}&"
            f"sortby=rank&"
            f"token={SN_API_KEY}"
        )
    else:  # regular
        url = (
            f"https://stocknewsapi.com/api/v1?"
            f"tickers={search_term}&"
            f"items={ITEMS_PER_PAGE}&"
            f"date={date_str}-{date_str}&"
            f"page={page}&"
            f"sortby=rank&"
            f"token={SN_API_KEY}"
        )
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        articles = data.get('data', [])
        
        # Log if we get unexpected response structure
        if 'data' not in data:
            logging.warning(f"  Unexpected API response structure for {search_term} on {date.strftime('%Y-%m-%d')}: {list(data.keys())}")
        
        return articles
    except requests.exceptions.HTTPError as e:
        # Log the error for debugging
        error_msg = f"HTTP {e.response.status_code}"
        if e.response.status_code == 422:
            logging.debug(f"  422 error for {search_term} on {date.strftime('%Y-%m-%d')} (invalid date/params)")
            return []  # Return empty list, not None, so fallback can still try
        else:
            logging.warning(f"  {error_msg} for {search_term} on {date.strftime('%Y-%m-%d')}: {e.response.text[:200]}")
            return None
    except Exception as e:
        logging.warning(f"  Error fetching articles for {search_term} on {date.strftime('%Y-%m-%d')}: {e}")
        return None

def verify_term_in_article(article, search_term):
    """
    Verify that the search term is mentioned in the article.
    Used for Regular endpoint articles that might mention multiple terms.
    """
    title = article.get('title', '').upper()
    text = article.get('text', '').upper()
    term_upper = search_term.upper()
    
    # Check if term appears in title or text
    return term_upper in title or term_upper in text

def verify_ticker_in_article(article, search_term):
    """
    Verify that the search term (ticker) is the primary source of the article.
    Used for Advanced endpoint articles.
    """
    title = article.get('title', '').upper()
    text = article.get('text', '').upper()
    ticker_upper = search_term.upper()
    
    # Check if the article is primarily about the ticker
    return ticker_upper in title and ticker_upper in text

def deduplicate_articles(articles):
    """
    Remove duplicate articles based on URL (most reliable) or title.
    """
    seen_urls = set()
    seen_titles = set()
    unique_articles = []
    
    for article in articles:
        url = article.get('url', '')
        title = article.get('title', '').strip()
        
        # Use URL as primary deduplication key
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)
        # Fallback to title if no URL
        elif title and title not in seen_titles:
            seen_titles.add(title)
            unique_articles.append(article)
    
    return unique_articles

def get_articles_for_day(ticker, date, company_name=None, abbrev1=None, abbrev2=None):
    """
    Get up to TARGET_ARTICLES_PER_DAY articles for a ticker on a specific date.
    Uses comprehensive fallback strategy with early stopping at 20 articles.
    Returns articles with 'search_source' field indicating which search found them.
    """
    all_articles = []
    endpoint_used = None
    search_terms_used = []
    
    # Step 1: Try Advanced endpoint with ticker (tickers-only) - page 1
    articles = fetch_articles_for_day(ticker, date, endpoint_type='advanced', page=1)
    if articles is None:
        # Error occurred, try regular endpoint as fallback
        logging.warning(f"  Advanced endpoint error for {ticker} on {date.strftime('%Y-%m-%d')}, trying regular endpoint...")
        articles = fetch_articles_for_day(ticker, date, endpoint_type='regular', page=1)
        if articles is None:
            pass  # Will try company name below
        elif articles:
            filtered = [a for a in articles if verify_ticker_in_article(a, ticker)]
            # Tag articles with search source
            for article in filtered:
                article['search_source'] = 'ticker'
            all_articles.extend(filtered)
            if filtered:
                endpoint_used = 'regular'
                search_terms_used.append(f"ticker:{ticker}")
    elif articles:
        # Tag articles with search source
        for article in articles:
            article['search_source'] = 'ticker'
        all_articles.extend(articles)
        endpoint_used = 'advanced'
        search_terms_used.append(f"ticker:{ticker}")
    
    # Check if we've hit target - STOP if so
    if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
        selected_articles = all_articles[:TARGET_ARTICLES_PER_DAY]
        return selected_articles, endpoint_used or 'none', search_terms_used
    
    # Step 2: If < FALLBACK_THRESHOLD, try Regular endpoint with ticker - page 1
    if len(all_articles) < FALLBACK_THRESHOLD:
        if f"ticker:{ticker}" not in search_terms_used or endpoint_used != 'regular':
            logging.info(f"  Ticker search returned {len(all_articles)} articles for {ticker} on {date.strftime('%Y-%m-%d')}, trying regular endpoint...")
            regular_articles = fetch_articles_for_day(ticker, date, endpoint_type='regular', page=1)
            if regular_articles is not None:
                filtered = [a for a in regular_articles if verify_term_in_article(a, ticker)]
                # Tag articles with search source (only new ones not already in all_articles)
                existing_urls = {a.get('url') for a in all_articles if a.get('url')}
                new_articles = [a for a in filtered if a.get('url') not in existing_urls]
                for article in new_articles:
                    article['search_source'] = 'ticker'
                all_articles.extend(new_articles)
                if new_articles:
                    endpoint_used = 'regular'
                    if f"ticker:{ticker}" not in search_terms_used:
                        search_terms_used.append(f"ticker:{ticker}")
                    logging.info(f"  Regular endpoint (ticker) found {len(new_articles)} additional articles")
    
    # Check if we've hit target - STOP if so
    if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
        selected_articles = all_articles[:TARGET_ARTICLES_PER_DAY]
        return selected_articles, endpoint_used or 'none', search_terms_used
    
    # Step 3: If still < FALLBACK_THRESHOLD, try Company Name (Regular endpoint)
    if len(all_articles) < FALLBACK_THRESHOLD and company_name:
        logging.info(f"  Trying company name search: '{company_name}' for {ticker} on {date.strftime('%Y-%m-%d')}...")
        company_articles = fetch_articles_for_day(company_name, date, endpoint_type='regular', page=1)
        if company_articles is not None:
            filtered = [a for a in company_articles if verify_term_in_article(a, company_name) or verify_term_in_article(a, ticker)]
            # Only add articles we don't already have
            existing_urls = {a.get('url') for a in all_articles if a.get('url')}
            new_articles = [a for a in filtered if a.get('url') not in existing_urls]
            # Tag new articles with search source
            for article in new_articles:
                article['search_source'] = 'company_name'
            all_articles.extend(new_articles)
            if new_articles:
                endpoint_used = 'regular'
                search_terms_used.append(f"company:{company_name}")
                logging.info(f"  Company name search found {len(new_articles)} additional articles")
    
    # Check if we've hit target - STOP if so
    if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
        selected_articles = all_articles[:TARGET_ARTICLES_PER_DAY]
        return selected_articles, endpoint_used or 'none', search_terms_used
    
    # Step 4: If still < FALLBACK_THRESHOLD, try Abbreviated Name 1 (Regular endpoint)
    if len(all_articles) < FALLBACK_THRESHOLD and abbrev1:
        logging.info(f"  Trying abbreviated name search: '{abbrev1}' for {ticker} on {date.strftime('%Y-%m-%d')}...")
        abbrev1_articles = fetch_articles_for_day(abbrev1, date, endpoint_type='regular', page=1)
        if abbrev1_articles is not None:
            filtered = [a for a in abbrev1_articles if verify_term_in_article(a, abbrev1) or verify_term_in_article(a, ticker)]
            # Only add articles we don't already have
            existing_urls = {a.get('url') for a in all_articles if a.get('url')}
            new_articles = [a for a in filtered if a.get('url') not in existing_urls]
            # Tag new articles with search source
            for article in new_articles:
                article['search_source'] = 'abbrev1'
            all_articles.extend(new_articles)
            if new_articles:
                endpoint_used = 'regular'
                search_terms_used.append(f"abbrev1:{abbrev1}")
                logging.info(f"  Abbreviated name 1 search found {len(new_articles)} additional articles")
    
    # Check if we've hit target - STOP if so
    if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
        selected_articles = all_articles[:TARGET_ARTICLES_PER_DAY]
        return selected_articles, endpoint_used or 'none', search_terms_used
    
    # Step 5: If still < FALLBACK_THRESHOLD, try Abbreviated Name 2 (Regular endpoint)
    if len(all_articles) < FALLBACK_THRESHOLD and abbrev2:
        logging.info(f"  Trying secondary abbreviated name search: '{abbrev2}' for {ticker} on {date.strftime('%Y-%m-%d')}...")
        abbrev2_articles = fetch_articles_for_day(abbrev2, date, endpoint_type='regular', page=1)
        if abbrev2_articles is not None:
            filtered = [a for a in abbrev2_articles if verify_term_in_article(a, abbrev2) or verify_term_in_article(a, ticker)]
            # Only add articles we don't already have
            existing_urls = {a.get('url') for a in all_articles if a.get('url')}
            new_articles = [a for a in filtered if a.get('url') not in existing_urls]
            # Tag new articles with search source
            for article in new_articles:
                article['search_source'] = 'abbrev2'
            all_articles.extend(new_articles)
            if new_articles:
                endpoint_used = 'regular'
                search_terms_used.append(f"abbrev2:{abbrev2}")
                logging.info(f"  Abbreviated name 2 search found {len(new_articles)} additional articles")
    
    # Check if we've hit target - STOP if so
    if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
        selected_articles = all_articles[:TARGET_ARTICLES_PER_DAY]
        return selected_articles, endpoint_used or 'none', search_terms_used
    
    # Deduplicate articles (important when using multiple search terms)
    all_articles = deduplicate_articles(all_articles)
    
    # Step 6: If still < TARGET_ARTICLES_PER_DAY, paginate (but check after each page)
    if len(all_articles) < TARGET_ARTICLES_PER_DAY:
        # Determine which search term to paginate (use ticker, most reliable)
        endpoint_to_use = endpoint_used if endpoint_used else 'advanced'
        search_term_to_use = ticker  # Default to ticker for pagination
        
        for page in range(2, MAX_PAGES + 1):
            if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
                break  # Stop if we hit target
            
            page_articles = fetch_articles_for_day(search_term_to_use, date, endpoint_type=endpoint_to_use, page=page)
            if page_articles is None:
                break  # Error occurred, stop paginating
            
            if not page_articles:
                break  # No more articles, stop paginating
            
            # Filter articles
            if endpoint_to_use == 'regular':
                page_articles = [a for a in page_articles if verify_term_in_article(a, ticker)]
            
            # Tag paginated articles (they came from ticker search pagination)
            for article in page_articles:
                if 'search_source' not in article:
                    article['search_source'] = 'ticker'
            
            all_articles.extend(page_articles)
            all_articles = deduplicate_articles(all_articles)  # Re-deduplicate after each page
            
            # Check if we've hit target after this page
            if len(all_articles) >= TARGET_ARTICLES_PER_DAY:
                break  # Stop paginating
            
            time.sleep(API_DELAY)  # Rate limiting between pages
    
    # Log if we got 0 articles
    if len(all_articles) == 0:
        logging.warning(f"  ⚠️  No articles found for {ticker} on {date.strftime('%Y-%m-%d')} (tried: {', '.join(search_terms_used) if search_terms_used else 'ticker only'})")
    
    # Select top TARGET_ARTICLES_PER_DAY articles (they're already sorted by rank)
    selected_articles = all_articles[:TARGET_ARTICLES_PER_DAY]
    
    return selected_articles, endpoint_used or 'none', search_terms_used

def scrape_stock_news(ticker, company_name, abbrev1=None, abbrev2=None):
    """
    Scrape news articles for a single ticker for the last LOOKBACK_DAYS days.
    Tracks which search terms found articles for monitoring effectiveness.
    """
    logging.info(f"\n{'='*70}")
    logging.info(f"Processing: {ticker} ({company_name})")
    logging.info(f"{'='*70}")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    
    all_articles = []
    total_calls = 0
    
    # Counters for tracking search source effectiveness
    search_counters = {
        'ticker': 0,
        'company_name': 0,
        'abbrev1': 0,
        'abbrev2': 0
    }
    
    # Create progress bar
    total_days = LOOKBACK_DAYS
    pbar = tqdm(total=total_days, desc=f"{ticker}", unit="day")
    
    # Check if we have progress for this ticker
    ticker_key = ticker
    if ticker_key in progress.get('current_ticker_progress', {}):
        last_processed_date = progress['current_ticker_progress'][ticker_key].get('last_date')
        if last_processed_date:
            # Resume from last processed date
            last_date = datetime.strptime(last_processed_date, '%Y-%m-%d')
            current_date = last_date + timedelta(days=1)  # FIX: was going backwards
            pbar.update((last_date - start_date).days)
        else:
            current_date = start_date
    else:
        current_date = start_date
    
    # Process each day
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Fetch articles for this day (now with company name fallbacks)
        articles, endpoint, search_terms = get_articles_for_day(
            ticker, current_date, company_name, abbrev1, abbrev2
        )
        total_calls += 1  # Count API calls (each search term is a call)
        
        # Count articles by search source
        for article in articles:
            source = article.get('search_source', 'ticker')  # Default to ticker if not set
            if source in search_counters:
                search_counters[source] += 1
        
        # Add metadata to each article
        for article in articles:
            article['ticker'] = ticker
            article['company_name'] = company_name
            article['query_date'] = date_str
            article['endpoint_used'] = endpoint
            article['search_terms_used'] = ', '.join(search_terms) if search_terms else 'ticker'
        
        all_articles.extend(articles)
        
        # Update progress
        progress['current_ticker_progress'][ticker_key] = {
            'last_date': date_str,
            'articles_collected': len(all_articles)
        }
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)
        
        # Update progress bar with search source counters
        pbar.update(1)
        pbar.set_postfix({
            'articles': len(all_articles),
            'calls': total_calls,
            'T': search_counters['ticker'],
            'C': search_counters['company_name'],
            'A1': search_counters['abbrev1'],
            'A2': search_counters['abbrev2']
        })
        
        # Rate limiting
        time.sleep(API_DELAY)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    pbar.close()
    
    # Log summary with search source breakdown
    total_articles = len(all_articles)
    logging.info(f"\n✅ {ticker}: Collected {total_articles} articles in {total_calls} API calls")
    logging.info(f"   Search Source Breakdown:")
    logging.info(f"     Ticker:           {search_counters['ticker']:5d} ({search_counters['ticker']/total_articles*100 if total_articles > 0 else 0:.1f}%)")
    if company_name:
        logging.info(f"     Company Name:     {search_counters['company_name']:5d} ({search_counters['company_name']/total_articles*100 if total_articles > 0 else 0:.1f}%)")
    if abbrev1:
        logging.info(f"     Abbrev 1:         {search_counters['abbrev1']:5d} ({search_counters['abbrev1']/total_articles*100 if total_articles > 0 else 0:.1f}%)")
    if abbrev2:
        logging.info(f"     Abbrev 2:         {search_counters['abbrev2']:5d} ({search_counters['abbrev2']/total_articles*100 if total_articles > 0 else 0:.1f}%)")
    
    return all_articles

def save_to_gcs(data, ticker):
    """
    Save articles to GCS Bronze layer as Parquet file.
    """
    if not data:
        logging.warning(f"⚠️  No data to save for {ticker}")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Ensure date column is datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    if 'query_date' in df.columns:
        df['query_date'] = pd.to_datetime(df['query_date'], errors='coerce')
    
    # Save to local parquet first
    local_parquet = os.path.join(project_root, "data", f"sn_news_{ticker}.parquet")
    os.makedirs(os.path.dirname(local_parquet), exist_ok=True)
    df.to_parquet(local_parquet, index=False)
    
    # Upload to GCS
    bucket = get_storage_client().bucket(GCS_BUCKET_NAME)
    gcs_path = f"bronze/news/stock_news_api/{ticker}/sn_news_{ticker}.parquet"
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_parquet)
    
    logging.info(f"✅ Uploaded {len(df)} articles to GCS: {gcs_path}")
    
    # Clean up local file
    os.remove(local_parquet)

# Main execution
if __name__ == "__main__":
    # Call the logging function
    log_collection_start()

    # Production mode: Process all tickers
    # Filter out already completed tickers
    remaining_tickers = [t for t in tickers if t not in progress.get('completed_tickers', [])]
    
    if not remaining_tickers:
        logging.info("✅ All tickers already processed!")
        exit(0)
    
    logging.info(f"Processing {len(remaining_tickers)} tickers...")
    logging.info("")
    
    for idx, row in stock_df.iterrows():
        ticker = row['Symbol']
        company_name = row.get('CompanyName', '')
        abbrev1 = row.get('Abbreviated Company Name', '') if pd.notna(row.get('Abbreviated Company Name', '')) else None
        abbrev2 = row.get('Abbreviated Secondary Company Name', '') if pd.notna(row.get('Abbreviated Secondary Company Name', '')) else None
        
        # Skip if already completed
        if ticker in progress.get('completed_tickers', []):
            continue
        
        try:
            # Scrape news for this ticker (now with company name fallbacks)
            articles = scrape_stock_news(ticker, company_name, abbrev1, abbrev2)
            
            # Save to GCS
            save_to_gcs(articles, ticker)
            
            # Mark as completed
            progress['completed_tickers'].append(ticker)
            if ticker in progress.get('current_ticker_progress', {}):
                del progress['current_ticker_progress'][ticker]
            
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(progress, f, indent=2)
            
            logging.info(f"✅ Completed {ticker}: {len(articles)} articles")
            
        except Exception as e:
            logging.error(f"❌ Error processing {ticker}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    logging.info("\n" + "="*70)
    logging.info("✅ DATA COLLECTION COMPLETE")
    logging.info("="*70)