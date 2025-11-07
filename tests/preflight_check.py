"""
preflight_check.py
────────────────────────────────────────────────────────────────────────
Author: Evan Callaghan
Created: 2025-11-06
Description: 
    Pre-flight checklist script to verify all prerequisites are met
    before running the Reddit ingestion pipeline test. This ensures
    production-level readiness.

License:
    All rights reserved. This repository is publicly accessible 
    for educational and portfolio demonstration purposes only.
"""
# ======================================================================
# Imports
# ======================================================================
import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# ======================================================================
# Logging Configuration
# ======================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ======================================================================
# Pre-flight Checks
# ======================================================================
def check_file_exists(filepath, description):
    """Check if a required file exists."""
    path = project_root / filepath
    exists = path.exists()
    status = "✅" if exists else "❌"
    if exists:
        logging.info(f"{status} {description}: {path}")
    else:
        logging.error(f"{status} {description}: {path}")
        logging.error(f"   → Missing file: {filepath}")
    return exists

def check_env_var(var_name, description):
    """Check if an environment variable is set."""
    from dotenv import load_dotenv
    load_dotenv()
    value = os.getenv(var_name)
    is_set = value is not None and value.strip() != ""
    status = "✅" if is_set else "❌"
    if is_set:
        logging.info(f"{status} {description}: {var_name}")
        if var_name in ["REDDIT_CLIENT_SECRET", "GCS_BUCKET_NAME"]:
            # Show partial value for security-sensitive vars
            logging.info(f"   → Value: {value[:10]}..." if len(value) > 10 else f"   → Value: {value}")
    else:
        logging.error(f"{status} {description}: {var_name}")
        logging.error(f"   → Missing environment variable: {var_name}")
    return is_set

def check_gcs_credentials():
    """Check if GCS credentials are configured."""
    try:
        from google.cloud import storage
        # Try to initialize client
        client = storage.Client()
        # Try to list buckets (this will fail if credentials are invalid)
        list(client.list_buckets(max_results=1))
        logging.info("✅ GCS credentials: Valid and accessible")
        return True
    except Exception as e:
        logging.error(f"❌ GCS credentials: Error - {str(e)}")
        logging.error(f"   → Check that GOOGLE_APPLICATION_CREDENTIALS is set or")
        logging.error(f"   → Ensure service account key file exists in gcp_keys/")
        return False

def check_reddit_auth():
    """Check if Reddit API credentials work."""
    try:
        import praw
        from dotenv import load_dotenv
        load_dotenv()
        
        reddit = praw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            user_agent=os.getenv("REDDIT_USER_AGENT")
        )
        reddit.read_only = True
        
        # Try to access Reddit (simple read operation)
        next(reddit.subreddit("test").hot(limit=1))
        logging.info("✅ Reddit API: Authentication successful")
        return True
    except Exception as e:
        logging.error(f"❌ Reddit API: Authentication failed - {str(e)}")
        logging.error(f"   → Verify REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, and REDDIT_USER_AGENT")
        return False

def check_dependencies():
    """Check if required Python packages are installed."""
    required = [
        "pytest", "pandas", "praw", "google.cloud.storage",
        "yaml", "tqdm", "dotenv"
    ]
    missing = []
    for package in required:
        try:
            __import__(package.replace(".", "_") if "." in package else package)
            logging.info(f"✅ Package: {package}")
        except ImportError:
            logging.error(f"❌ Package: {package} (not installed)")
            missing.append(package)
    
    if missing:
        logging.warning(f"   → Install missing packages: pip install {' '.join(missing)}")
    return len(missing) == 0

def main():
    """Run all pre-flight checks."""
    logging.info("=" * 70)
    logging.info("REDDIT INGESTION PIPELINE - PRE-FLIGHT CHECK")
    logging.info("=" * 70)
    logging.info("")
    
    all_checks_passed = True
    
    # File checks
    logging.info("📁 FILE CHECKS")
    logging.info("-" * 70)
    all_checks_passed &= check_file_exists(
        "config/sp500_metadata.csv",
        "S&P 500 metadata file"
    )
    all_checks_passed &= check_file_exists(
        "config/reddit_subs.yaml",
        "Reddit subreddits config"
    )
    all_checks_passed &= check_file_exists(
        "fetch_data/fetch_reddit.py",
        "Reddit ingestion script"
    )
    all_checks_passed &= check_file_exists(
        "tests/test_fetch_reddit_pipeline.py",
        "Test file"
    )
    logging.info("")
    
    # Environment variable checks
    logging.info("🔐 ENVIRONMENT VARIABLES")
    logging.info("-" * 70)
    all_checks_passed &= check_env_var(
        "REDDIT_CLIENT_ID",
        "Reddit Client ID"
    )
    all_checks_passed &= check_env_var(
        "REDDIT_CLIENT_SECRET",
        "Reddit Client Secret"
    )
    all_checks_passed &= check_env_var(
        "REDDIT_USER_AGENT",
        "Reddit User Agent"
    )
    all_checks_passed &= check_env_var(
        "GCS_BUCKET_NAME",
        "GCS Bucket Name"
    )
    logging.info("")
    
    # Dependency checks
    logging.info("📦 DEPENDENCIES")
    logging.info("-" * 70)
    all_checks_passed &= check_dependencies()
    logging.info("")
    
    # Authentication checks
    logging.info("🔑 AUTHENTICATION")
    logging.info("-" * 70)
    all_checks_passed &= check_reddit_auth()
    all_checks_passed &= check_gcs_credentials()
    logging.info("")
    
    # Summary
    logging.info("=" * 70)
    if all_checks_passed:
        logging.info("✅ ALL CHECKS PASSED - Ready to run tests!")
        logging.info("")
        logging.info("Next steps:")
        logging.info("  1. Run: pytest tests/test_fetch_reddit_pipeline.py -v")
        logging.info("  2. Review test output for data quality metrics")
        logging.info("  3. Optionally verify data in Databricks notebook")
        return 0
    else:
        logging.error("❌ SOME CHECKS FAILED - Please fix issues above before running tests")
        return 1

if __name__ == "__main__":
    sys.exit(main())

