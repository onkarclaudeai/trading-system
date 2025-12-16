#!/usr/bin/env python3
"""
NSE F&O Bhav Copy Downloader - Fixed Version
Downloads historical F&O data (not future dates)
"""

import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
from datetime import datetime, timedelta
import zipfile
import io
import logging
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')
}


def download_fo_bhav_copy(date: datetime) -> Optional[bytes]:
    """Download F&O bhav copy for a specific date."""
    
    # Skip future dates
    if date.date() > datetime.now().date():
        logger.warning(f"Skipping future date: {date.date()}")
        return None
    
    # Format: fo27NOV2024bhav.csv.zip (not 2025!)
    date_str = date.strftime("%d%b%Y").upper()
    month = date.strftime("%b").upper()
    year = date.strftime("%Y")
    
    # Try current year structure first
    url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date_str}bhav.csv.zip"
    
    logger.info(f"Attempting to download from: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            logger.info(f"✅ Downloaded F&O bhav copy for {date_str}")
            return response.content
        elif response.status_code == 404:
            # Try alternative URL structure (for older data)
            url2 = f"https://www1.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date_str}bhav.csv.zip"
            logger.info(f"First URL failed, trying: {url2}")
            response = requests.get(url2, headers=headers, timeout=30)
            if response.status_code == 200:
                logger.info(f"✅ Downloaded from alternative URL")
                return response.content
            else:
                logger.warning(f"F&O data not available for {date.date()}")
                return None
        else:
            logger.warning(f"Failed with status {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error downloading: {e}")
        return None


def main():
    """Main function to download F&O data."""
    
    logger.info("Starting NSE F&O Bhav Copy download...")
    logger.info(f"Current date: {datetime.now().date()}")
    
    # Important: Download HISTORICAL data (not future!)
    # F&O bhav copy is usually available after market close (6 PM)
    # So we should look for yesterday's data or earlier
    
    today = datetime.now()
    
    # Try to get last 5 trading days (going backwards)
    successful = 0
    dates_to_try = []
    
    for days_back in range(1, 10):  # Start from yesterday
        check_date = today - timedelta(days=days_back)
        # Skip weekends
        if check_date.weekday() < 5:  # Monday = 0, Friday = 4
            dates_to_try.append(check_date)
            if len(dates_to_try) >= 5:
                break
    
    logger.info(f"Will try to download F&O data for: {[d.date() for d in dates_to_try]}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Create tables
        from nse_fo_downloader import create_fo_tables, extract_and_parse_fo_csv, process_fo_data, insert_fo_data, calculate_fo_analysis
        create_fo_tables(conn)
        
        for date in dates_to_try:
            try:
                # Check if we already have this data
                check_query = "SELECT COUNT(*) FROM fo_bhav_copy WHERE trading_date = %s"
                with conn.cursor() as cur:
                    cur.execute(check_query, (date.date(),))
                    count = cur.fetchone()[0]
                
                if count > 0:
                    logger.info(f"Already have F&O data for {date.date()}, skipping")
                    continue
                
                # Download
                zip_content = download_fo_bhav_copy(date)
                
                if zip_content:
                    # Parse and save
                    df = extract_and_parse_fo_csv(zip_content)
                    
                    if not df.empty:
                        records = process_fo_data(df, date)
                        
                        if records:
                            insert_fo_data(conn, records)
                            calculate_fo_analysis(conn, date)
                            successful += 1
                            logger.info(f"✅ Processed {date.date()}: {len(records)} records")
            
            except Exception as e:
                logger.error(f"Error processing {date.date()}: {e}")
                continue
        
        logger.info(f"\nF&O Download Summary: Successfully downloaded {successful} days of data")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
