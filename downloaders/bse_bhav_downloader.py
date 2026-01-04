#!/usr/bin/env python
# coding: utf-8

# In[7]:


#!/usr/bin/env python3
"""
BSE Bhav Copy Downloader and PostgreSQL Importer
Downloads daily Bhav copy files from BSE for a specified date range
and inserts the data into a PostgreSQL database.

Can be run as a script or in Jupyter Notebook.
"""

import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import csv
import zipfile
import io
import logging
from typing import List, Dict
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True  # Force reconfiguration for Jupyter
)
logger = logging.getLogger(__name__)

# Database configuration
# Default configuration for localhost with default postgres database
# You can modify these values directly or use environment variables to override
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'postgres'),  # Using default postgres database
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres')  # Change this to your actual password
}

# BSE Bhav Copy URL pattern
# Format: https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_YYYYMMDD_F_0000.CSV
BSE_URL_TEMPLATE = "https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date}_F_0000.CSV"


def create_table_if_not_exists(conn):
    """Create the bhav_copy table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bhav_copy (
        id SERIAL PRIMARY KEY,
        sc_code VARCHAR(20),
        sc_name VARCHAR(200),
        sc_group VARCHAR(10),
        sc_type VARCHAR(10),
        open_price DECIMAL(15, 2),
        high_price DECIMAL(15, 2),
        low_price DECIMAL(15, 2),
        close_price DECIMAL(15, 2),
        last_price DECIMAL(15, 2),
        prev_close DECIMAL(15, 2),
        no_of_shrs BIGINT,
        no_of_trades INTEGER,
        net_turnover DECIMAL(20, 2),
        tdcloindi VARCHAR(10),
        isin_code VARCHAR(20),
        trading_date DATE,
        filler1 VARCHAR(50),
        filler2 VARCHAR(50),
        filler3 VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(sc_code, trading_date)
    );
    
    CREATE INDEX IF NOT EXISTS idx_bhav_trading_date ON bhav_copy(trading_date);
    CREATE INDEX IF NOT EXISTS idx_bhav_sc_code ON bhav_copy(sc_code);
    CREATE INDEX IF NOT EXISTS idx_bhav_isin ON bhav_copy(isin_code);
    """
    
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()
    logger.info("Table 'bhav_copy' checked/created successfully")


def download_bhav_copy(date: datetime) -> bytes:
    """Download Bhav copy for a specific date."""
    # Format: YYYYMMDD
    date_str = date.strftime("%Y%m%d")
    url = BSE_URL_TEMPLATE.format(date=date_str)
    
    logger.info(f"Downloading Bhav copy for {date.strftime('%Y-%m-%d')} from {url}")
    
    # Add headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.bseindia.com/'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download for {date.strftime('%Y-%m-%d')}: {e}")
        return None


def extract_csv_from_zip(content: bytes) -> str:
    """
    Extract CSV content. BSE provides direct CSV files.
    """
    try:
        csv_content = content.decode('utf-8')
        logger.info("Successfully decoded CSV content")
        return csv_content
    except UnicodeDecodeError as e:
        logger.error(f"Error decoding CSV content: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing content: {e}")
        return None


def parse_csv_data(csv_content: str, trading_date: datetime) -> List[tuple]:
    """Parse CSV content and return list of tuples for database insertion."""
    records = []
    csv_reader = csv.DictReader(io.StringIO(csv_content))
    
    for row in csv_reader:
        try:
            # New BSE CSV format uses different column names
            # Map new columns to our database fields
            sc_code = row.get('FinInstrmId', '').strip()  # Security code
            sc_name = row.get('FinInstrmNm', '').strip()  # Security name
            sc_group = row.get('SctySrs', '').strip()     # Security series/group
            sc_type = row.get('FinInstrmTp', '').strip()  # Financial instrument type
            
            # Skip if essential fields are missing
            if not sc_code or not sc_name:
                continue
            
            # Price fields
            open_price = float(row.get('OpnPric', 0) or 0)
            high_price = float(row.get('HghPric', 0) or 0)
            low_price = float(row.get('LwPric', 0) or 0)
            close_price = float(row.get('ClsPric', 0) or 0)
            last_price = float(row.get('LastPric', 0) or 0)
            prev_close = float(row.get('PrvsClsgPric', 0) or 0)
            
            # Volume and trade fields
            no_of_shrs = int(row.get('TtlTradgVol', 0) or 0)
            no_of_trades = int(row.get('TtlNbOfTxsExctd', 0) or 0)
            net_turnover = float(row.get('TtlTrfVal', 0) or 0)
            
            # Other fields
            tdcloindi = ''  # Not in new format
            isin_code = row.get('ISIN', '').strip()
            
            record = (
                sc_code,
                sc_name,
                sc_group,
                sc_type,
                open_price,
                high_price,
                low_price,
                close_price,
                last_price,
                prev_close,
                no_of_shrs,
                no_of_trades,
                net_turnover,
                tdcloindi,
                isin_code,
                trading_date.date(),
                '',  # filler1
                '',  # filler2
                ''   # filler3
            )
            records.append(record)
        except Exception as e:
            logger.warning(f"Error parsing row: {e}, Row: {row}")
            continue
    
    return records


def insert_data(conn, records: List[tuple]):
    """Insert records into PostgreSQL database."""
    insert_query = """
    INSERT INTO bhav_copy (
        sc_code, sc_name, sc_group, sc_type, open_price, high_price, 
        low_price, close_price, last_price, prev_close, no_of_shrs, 
        no_of_trades, net_turnover, tdcloindi, isin_code, trading_date,
        filler1, filler2, filler3
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (sc_code, trading_date) DO NOTHING;
    """
    
    with conn.cursor() as cur:
        execute_batch(cur, insert_query, records, page_size=1000)
    conn.commit()
    logger.info(f"Inserted {len(records)} records (duplicates skipped)")


def process_date_range(start_date: datetime, end_date: datetime):
    """Process Bhav copies for a date range."""
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        create_table_if_not_exists(conn)
        
        current_date = start_date
        successful = 0
        failed = 0
        
        while current_date <= end_date:
            # Skip weekends (BSE is closed)
            if current_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
                logger.info(f"Skipping {current_date.strftime('%Y-%m-%d')} (weekend)")
                current_date += timedelta(days=1)
                continue
            
            try:
                # Download ZIP file
                zip_content = download_bhav_copy(current_date)
                
                if zip_content:
                    # Extract CSV
                    csv_content = extract_csv_from_zip(zip_content)
                    
                    if csv_content:
                        # Parse and insert data
                        records = parse_csv_data(csv_content, current_date)
                        
                        if records:
                            insert_data(conn, records)
                            successful += 1
                            logger.info(f"Successfully processed {current_date.strftime('%Y-%m-%d')}")
                        else:
                            logger.warning(f"No valid records found for {current_date.strftime('%Y-%m-%d')}")
                            failed += 1
                    else:
                        failed += 1
                else:
                    failed += 1
                
                # Be respectful to BSE servers
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error processing {current_date.strftime('%Y-%m-%d')}: {e}")
                failed += 1
            
            current_date += timedelta(days=1)
        
        logger.info(f"\nSummary: {successful} successful, {failed} failed")
        
    finally:
        conn.close()


def main():
    """Main function to run the script."""
    # Calculate date range (last 6 months)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)  # Approximately 6 months
    
    logger.info(f"Starting BSE Bhav Copy download and import")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    process_date_range(start_date, end_date)
    
    logger.info("Process completed")


def run_with_custom_dates(start_date_str: str, end_date_str: str):
    """
    Run with custom date range (useful for Jupyter).
    
    Args:
        start_date_str: Start date in 'YYYY-MM-DD' format
        end_date_str: End date in 'YYYY-MM-DD' format
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    logger.info(f"Starting BSE Bhav Copy download and import")
    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    process_date_range(start_date, end_date)
    
    logger.info("Process completed")


def set_db_config(host='localhost', port='5432', database='bse_data', user='postgres', password=''):
    """
    Set database configuration (useful for Jupyter).
    
    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Database user
        password: Database password
    """
    global DB_CONFIG
    DB_CONFIG = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }
    logger.info(f"Database config updated: {host}:{port}/{database}")


if __name__ == "__main__":
    main()


# In[ ]:
