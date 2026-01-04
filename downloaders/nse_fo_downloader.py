#!/usr/bin/env python3
"""
NSE F&O Bhav Copy Downloader - Fixed Version
Downloads historical F&O data from NSE archives.

FIXES:
- Removed circular import bug (was importing from itself)
- Added all missing functions: create_fo_tables, extract_and_parse_fo_csv, 
  process_fo_data, insert_fo_data, calculate_fo_analysis
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
from typing import List, Dict, Optional, Tuple

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


def create_fo_tables(conn):
    """Create F&O related tables if they don't exist."""
    
    create_tables_sql = """
    -- Main F&O bhav copy table
    CREATE TABLE IF NOT EXISTS fo_bhav_copy (
        id SERIAL PRIMARY KEY,
        trading_date DATE NOT NULL,
        instrument VARCHAR(20),
        symbol VARCHAR(50),
        expiry_date DATE,
        strike_price DECIMAL(15, 2),
        option_type VARCHAR(10),
        open_price DECIMAL(15, 2),
        high_price DECIMAL(15, 2),
        low_price DECIMAL(15, 2),
        close_price DECIMAL(15, 2),
        settle_price DECIMAL(15, 2),
        contracts BIGINT,
        value_in_lakhs DECIMAL(20, 2),
        open_interest BIGINT,
        change_in_oi BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(trading_date, symbol, expiry_date, strike_price, option_type)
    );
    
    -- Index for faster queries
    CREATE INDEX IF NOT EXISTS idx_fo_trading_date ON fo_bhav_copy(trading_date);
    CREATE INDEX IF NOT EXISTS idx_fo_symbol ON fo_bhav_copy(symbol);
    CREATE INDEX IF NOT EXISTS idx_fo_expiry ON fo_bhav_copy(expiry_date);
    CREATE INDEX IF NOT EXISTS idx_fo_instrument ON fo_bhav_copy(instrument);
    
    -- F&O analysis summary table
    CREATE TABLE IF NOT EXISTS fo_analysis (
        id SERIAL PRIMARY KEY,
        trading_date DATE NOT NULL,
        symbol VARCHAR(50),
        total_contracts BIGINT,
        total_oi BIGINT,
        net_oi_change BIGINT,
        put_call_ratio DECIMAL(10, 4),
        max_pain_strike DECIMAL(15, 2),
        futures_oi BIGINT,
        futures_oi_change BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(trading_date, symbol)
    );
    
    CREATE INDEX IF NOT EXISTS idx_fo_analysis_date ON fo_analysis(trading_date);
    CREATE INDEX IF NOT EXISTS idx_fo_analysis_symbol ON fo_analysis(symbol);
    """
    
    with conn.cursor() as cur:
        cur.execute(create_tables_sql)
    conn.commit()
    logger.info("F&O tables created/verified successfully")


def extract_and_parse_fo_csv(zip_content: bytes) -> pd.DataFrame:
    """Extract CSV from ZIP and parse into DataFrame."""
    
    try:
        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # Get the CSV filename (usually only one file in the ZIP)
            csv_filename = [f for f in zf.namelist() if f.endswith('.csv')][0]
            
            with zf.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)
        
        logger.info(f"Extracted CSV with {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting/parsing CSV: {e}")
        return pd.DataFrame()


def process_fo_data(df: pd.DataFrame, trading_date: datetime) -> List[Tuple]:
    """Process DataFrame and return list of tuples for database insertion."""
    
    records = []
    
    # NSE F&O CSV column mapping (may vary slightly)
    column_mapping = {
        'INSTRUMENT': 'instrument',
        'SYMBOL': 'symbol',
        'EXPIRY_DT': 'expiry_date',
        'STRIKE_PR': 'strike_price',
        'OPTION_TYP': 'option_type',
        'OPEN': 'open_price',
        'HIGH': 'high_price',
        'LOW': 'low_price',
        'CLOSE': 'close_price',
        'SETTLE_PR': 'settle_price',
        'CONTRACTS': 'contracts',
        'VAL_INLAKH': 'value_in_lakhs',
        'OPEN_INT': 'open_interest',
        'CHG_IN_OI': 'change_in_oi'
    }
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.upper()
    
    for _, row in df.iterrows():
        try:
            # Parse expiry date
            expiry_str = str(row.get('EXPIRY_DT', '')).strip()
            try:
                expiry_date = datetime.strptime(expiry_str, '%d-%b-%Y').date()
            except:
                try:
                    expiry_date = datetime.strptime(expiry_str, '%d-%B-%Y').date()
                except:
                    expiry_date = None
            
            record = (
                trading_date.date(),
                str(row.get('INSTRUMENT', '')).strip(),
                str(row.get('SYMBOL', '')).strip(),
                expiry_date,
                float(row.get('STRIKE_PR', 0) or 0),
                str(row.get('OPTION_TYP', '')).strip(),
                float(row.get('OPEN', 0) or 0),
                float(row.get('HIGH', 0) or 0),
                float(row.get('LOW', 0) or 0),
                float(row.get('CLOSE', 0) or 0),
                float(row.get('SETTLE_PR', 0) or 0),
                int(row.get('CONTRACTS', 0) or 0),
                float(row.get('VAL_INLAKH', 0) or 0),
                int(row.get('OPEN_INT', 0) or 0),
                int(row.get('CHG_IN_OI', 0) or 0)
            )
            records.append(record)
            
        except Exception as e:
            logger.debug(f"Error processing row: {e}")
            continue
    
    logger.info(f"Processed {len(records)} F&O records")
    return records


def insert_fo_data(conn, records: List[Tuple]):
    """Insert F&O records into database."""
    
    insert_sql = """
    INSERT INTO fo_bhav_copy (
        trading_date, instrument, symbol, expiry_date, strike_price,
        option_type, open_price, high_price, low_price, close_price,
        settle_price, contracts, value_in_lakhs, open_interest, change_in_oi
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (trading_date, symbol, expiry_date, strike_price, option_type) 
    DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        settle_price = EXCLUDED.settle_price,
        contracts = EXCLUDED.contracts,
        value_in_lakhs = EXCLUDED.value_in_lakhs,
        open_interest = EXCLUDED.open_interest,
        change_in_oi = EXCLUDED.change_in_oi
    """
    
    with conn.cursor() as cur:
        execute_batch(cur, insert_sql, records, page_size=1000)
    conn.commit()
    logger.info(f"Inserted/updated {len(records)} F&O records")


def calculate_fo_analysis(conn, trading_date: datetime):
    """Calculate F&O analysis metrics for each symbol."""
    
    analysis_sql = """
    INSERT INTO fo_analysis (
        trading_date, symbol, total_contracts, total_oi, net_oi_change,
        put_call_ratio, max_pain_strike, futures_oi, futures_oi_change
    )
    SELECT 
        trading_date,
        symbol,
        SUM(contracts) as total_contracts,
        SUM(open_interest) as total_oi,
        SUM(change_in_oi) as net_oi_change,
        -- Put-Call Ratio (OI based)
        COALESCE(
            SUM(CASE WHEN option_type = 'PE' THEN open_interest ELSE 0 END)::DECIMAL /
            NULLIF(SUM(CASE WHEN option_type = 'CE' THEN open_interest ELSE 0 END), 0),
            0
        ) as put_call_ratio,
        -- Max Pain (simplified - strike with highest OI)
        (SELECT strike_price FROM fo_bhav_copy f2 
         WHERE f2.trading_date = fo_bhav_copy.trading_date 
         AND f2.symbol = fo_bhav_copy.symbol
         AND f2.option_type IN ('CE', 'PE')
         GROUP BY strike_price
         ORDER BY SUM(open_interest) DESC
         LIMIT 1) as max_pain_strike,
        -- Futures OI
        SUM(CASE WHEN instrument IN ('FUTIDX', 'FUTSTK') THEN open_interest ELSE 0 END) as futures_oi,
        SUM(CASE WHEN instrument IN ('FUTIDX', 'FUTSTK') THEN change_in_oi ELSE 0 END) as futures_oi_change
    FROM fo_bhav_copy
    WHERE trading_date = %s
    GROUP BY trading_date, symbol
    ON CONFLICT (trading_date, symbol) DO UPDATE SET
        total_contracts = EXCLUDED.total_contracts,
        total_oi = EXCLUDED.total_oi,
        net_oi_change = EXCLUDED.net_oi_change,
        put_call_ratio = EXCLUDED.put_call_ratio,
        max_pain_strike = EXCLUDED.max_pain_strike,
        futures_oi = EXCLUDED.futures_oi,
        futures_oi_change = EXCLUDED.futures_oi_change
    """
    
    with conn.cursor() as cur:
        cur.execute(analysis_sql, (trading_date.date(),))
    conn.commit()
    logger.info(f"Calculated F&O analysis for {trading_date.date()}")


def download_fo_bhav_copy(date: datetime) -> Optional[bytes]:
    """Download F&O bhav copy for a specific date."""
    
    # Skip future dates
    if date.date() > datetime.now().date():
        logger.warning(f"Skipping future date: {date.date()}")
        return None
    
    # Format: fo27NOV2024bhav.csv.zip
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


def download_date_range(start_date: datetime, end_date: datetime, conn=None):
    """Download F&O data for a date range."""
    
    close_conn = False
    if conn is None:
        conn = psycopg2.connect(**DB_CONFIG)
        close_conn = True
    
    try:
        create_fo_tables(conn)
        
        successful = 0
        current_date = start_date
        
        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue
            
            # Skip future dates
            if current_date.date() > datetime.now().date():
                current_date += timedelta(days=1)
                continue
            
            try:
                # Check if data already exists
                check_query = "SELECT COUNT(*) FROM fo_bhav_copy WHERE trading_date = %s"
                with conn.cursor() as cur:
                    cur.execute(check_query, (current_date.date(),))
                    count = cur.fetchone()[0]
                
                if count > 0:
                    logger.info(f"Already have data for {current_date.date()}, skipping")
                    current_date += timedelta(days=1)
                    continue
                
                # Download
                zip_content = download_fo_bhav_copy(current_date)
                
                if zip_content:
                    df = extract_and_parse_fo_csv(zip_content)
                    
                    if not df.empty:
                        records = process_fo_data(df, current_date)
                        
                        if records:
                            insert_fo_data(conn, records)
                            calculate_fo_analysis(conn, current_date)
                            successful += 1
                            logger.info(f"✅ Processed {current_date.date()}: {len(records)} records")
                
                # Rate limiting
                import time
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing {current_date.date()}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Download complete: {successful} days processed")
        return successful
        
    finally:
        if close_conn:
            conn.close()


def get_fo_summary(conn, symbol: str, trading_date: datetime = None) -> Dict:
    """Get F&O summary for a symbol."""
    
    if trading_date is None:
        trading_date = datetime.now()
    
    query = """
    SELECT * FROM fo_analysis 
    WHERE symbol = %s AND trading_date = %s
    """
    
    with conn.cursor() as cur:
        cur.execute(query, (symbol, trading_date.date()))
        row = cur.fetchone()
        
        if row:
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row))
    
    return {}


def set_db_config(host='localhost', port='5432', database='postgres',
                 user='postgres', password='postgres'):
    """Set database configuration."""
    global DB_CONFIG
    DB_CONFIG = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    logger.info(f"Database config updated: {host}:{port}/{database}")


def main():
    """Main function to download F&O data."""
    
    logger.info("Starting NSE F&O Bhav Copy download...")
    logger.info(f"Current date: {datetime.now().date()}")
    
    # Download HISTORICAL data (not future!)
    # F&O bhav copy is usually available after market close (6 PM)
    today = datetime.now()
    
    # Try to get last 5 trading days (going backwards)
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
        create_fo_tables(conn)
        
        successful = 0
        
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
