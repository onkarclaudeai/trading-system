#!/usr/bin/env python3
"""
Simplified Data Gap Filler
Identifies missing trading days and fills gaps in bhav_copy and watchlists.
Essential for maintaining data quality when daily workflow is missed.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from datetime import datetime, timedelta
import logging
import requests
import zipfile
import io
from typing import List, Dict, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
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

# NSE Holidays for 2024-2025 (add more as needed)
NSE_HOLIDAYS = [
    # 2024
    datetime(2024, 1, 26),   # Republic Day
    datetime(2024, 3, 8),    # Mahashivratri
    datetime(2024, 3, 25),   # Holi
    datetime(2024, 3, 29),   # Good Friday
    datetime(2024, 4, 11),   # Id-ul-Fitr
    datetime(2024, 4, 17),   # Ram Navami
    datetime(2024, 5, 1),    # Maharashtra Day
    datetime(2024, 6, 17),   # Bakri Id
    datetime(2024, 8, 15),   # Independence Day
    datetime(2024, 10, 2),   # Gandhi Jayanti
    datetime(2024, 11, 1),   # Diwali
    datetime(2024, 11, 15),  # Guru Nanak Jayanti
    # 2025
    datetime(2025, 1, 26),   # Republic Day
    datetime(2025, 3, 14),   # Holi
    datetime(2025, 3, 31),   # Id-ul-Fitr
    datetime(2025, 4, 18),   # Good Friday
    datetime(2025, 5, 1),    # Maharashtra Day
    datetime(2025, 8, 15),   # Independence Day
    datetime(2025, 10, 2),   # Gandhi Jayanti
]


class DataGapFiller:
    """Find and fill gaps in trading data."""
    
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.gaps_report = {
            'bhav_copy': [],
            'watchlist': [],
            'paper_trades': []
        }
    
    def get_expected_trading_days(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Get list of expected trading days (excluding weekends and holidays)."""
        
        trading_days = []
        current = start_date
        
        while current <= end_date:
            # Skip weekends (Saturday=5, Sunday=6)
            if current.weekday() < 5:
                # Skip holidays
                is_holiday = False
                for holiday in NSE_HOLIDAYS:
                    if (current.year == holiday.year and 
                        current.month == holiday.month and 
                        current.day == holiday.day):
                        is_holiday = True
                        break
                
                if not is_holiday:
                    trading_days.append(current)
            
            current += timedelta(days=1)
        
        return trading_days
    
    def find_bhav_copy_gaps(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Find missing bhav copy data."""
        
        logger.info(f"Checking bhav_copy gaps from {start_date.date()} to {end_date.date()}")
        
        # Get expected trading days
        expected_days = self.get_expected_trading_days(start_date, end_date)
        
        # Get existing dates from database
        query = """
        SELECT DISTINCT trading_date 
        FROM bhav_copy 
        WHERE trading_date BETWEEN %s AND %s
        ORDER BY trading_date
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (start_date.date(), end_date.date()))
            existing_dates = {row[0] for row in cur.fetchall()}
        
        # Find gaps
        gaps = []
        for day in expected_days:
            if day.date() not in existing_dates:
                gaps.append(day)
        
        self.gaps_report['bhav_copy'] = gaps
        logger.info(f"Found {len(gaps)} missing days in bhav_copy")
        
        return gaps
    
    def find_watchlist_gaps(self, start_date: datetime, end_date: datetime) -> List[datetime]:
        """Find missing watchlist predictions."""
        
        logger.info(f"Checking watchlist gaps from {start_date.date()} to {end_date.date()}")
        
        # Check if watchlist_predictions table exists
        check_table = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'watchlist_predictions'
        )
        """
        
        with self.conn.cursor() as cur:
            cur.execute(check_table)
            if not cur.fetchone()[0]:
                logger.info("Watchlist predictions table doesn't exist yet")
                return []
        
        # Get expected trading days
        expected_days = self.get_expected_trading_days(start_date, end_date)
        
        # Get existing watchlist dates
        query = """
        SELECT DISTINCT prediction_date 
        FROM watchlist_predictions 
        WHERE prediction_date BETWEEN %s AND %s
        ORDER BY prediction_date
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (start_date.date(), end_date.date()))
            existing_dates = {row[0] for row in cur.fetchall()}
        
        # Find gaps
        gaps = []
        for day in expected_days:
            if day.date() not in existing_dates:
                gaps.append(day)
        
        self.gaps_report['watchlist'] = gaps
        logger.info(f"Found {len(gaps)} missing days in watchlist predictions")
        
        return gaps
    
    def download_bhav_copy(self, date: datetime) -> bool:
        """Download bhav copy for a specific date."""
        
        try:
            # Format: EQ_ISIN_ddmmyyyy_CSV.ZIP
            date_str = date.strftime('%d%m%Y')
            
            # Try different URL patterns (BSE changes them occasionally)
            urls = [
                f"https://www.bseindia.com/download/BhavCopy/Equity/EQ_ISIN_{date_str}_CSV.ZIP",
                f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_EQ_{date_str}_CSV.ZIP",
                f"https://www.bseindia.com/download/BhavCopy/Equity/EQ_{date_str}_CSV.ZIP"
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            zip_content = None
            for url in urls:
                try:
                    response = requests.get(url, headers=headers, timeout=30)
                    if response.status_code == 200:
                        zip_content = response.content
                        logger.debug(f"Downloaded from: {url}")
                        break
                except:
                    continue
            
            if not zip_content:
                logger.warning(f"Could not download bhav copy for {date.date()}")
                return False
            
            # Extract and parse CSV
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                csv_filename = zf.namelist()[0]
                with zf.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file)
            
            # Process and insert data
            records = []
            for _, row in df.iterrows():
                try:
                    record = (
                        date.date(),  # trading_date
                        str(row.get('SC_CODE', '')),
                        str(row.get('SC_NAME', '')),
                        str(row.get('SC_GROUP', '')),
                        str(row.get('SC_TYPE', '')),
                        float(row.get('OPEN', 0) or 0),
                        float(row.get('HIGH', 0) or 0),
                        float(row.get('LOW', 0) or 0),
                        float(row.get('CLOSE', 0) or 0),
                        float(row.get('LAST', 0) or 0),
                        float(row.get('PREVCLOSE', 0) or 0),
                        int(row.get('NO_TRADES', 0) or 0),
                        int(row.get('NO_OF_SHRS', 0) or 0),
                        float(row.get('NET_TURNOV', 0) or 0),
                        str(row.get('ISIN_CODE', ''))
                    )
                    records.append(record)
                except Exception as e:
                    logger.debug(f"Error processing row: {e}")
                    continue
            
            if records:
                # Insert into database
                insert_query = """
                INSERT INTO bhav_copy (
                    trading_date, sc_code, sc_name, sc_group, sc_type,
                    open_price, high_price, low_price, close_price,
                    last_price, prev_close, no_of_trades, no_of_shrs,
                    net_turnover, isin_code
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (trading_date, sc_code) DO NOTHING
                """
                
                from psycopg2.extras import execute_batch
                with self.conn.cursor() as cur:
                    execute_batch(cur, insert_query, records, page_size=1000)
                
                self.conn.commit()
                logger.info(f"✅ Downloaded {date.date()}: {len(records)} records")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error downloading bhav copy for {date.date()}: {e}")
            return False
    
    def fill_bhav_copy_gaps(self, gaps: List[datetime], max_days: int = 30) -> int:
        """Fill missing bhav copy data."""
        
        if not gaps:
            logger.info("No bhav copy gaps to fill")
            return 0
        
        # Limit to recent gaps (avoid downloading too much old data)
        recent_gaps = [g for g in gaps if g >= datetime.now() - timedelta(days=max_days)]
        
        logger.info(f"Filling {len(recent_gaps)} bhav copy gaps (last {max_days} days)")
        
        successful = 0
        for gap_date in recent_gaps:
            if self.download_bhav_copy(gap_date):
                successful += 1
            
            # Small delay to avoid rate limiting
            import time
            time.sleep(1)
        
        logger.info(f"Successfully filled {successful}/{len(recent_gaps)} bhav copy gaps")
        return successful
    
    def generate_historical_watchlist(self, date: datetime) -> bool:
        """Generate watchlist for a historical date."""
        
        try:
            # Check if we have bhav_copy data for this date and previous days
            check_query = """
            SELECT COUNT(*) FROM bhav_copy 
            WHERE trading_date = %s
            """
            
            with self.conn.cursor() as cur:
                cur.execute(check_query, (date.date(),))
                if cur.fetchone()[0] == 0:
                    logger.warning(f"No bhav_copy data for {date.date()}")
                    return False
            
            # Run the watchlist generation script for this date
            # This is a simplified version - adjust based on your actual logic
            logger.info(f"Generating watchlist for {date.date()}")
            
            # Import your watchlist generation logic
            try:
                import intraday_options_watchlist
                # Call with specific date parameter if your script supports it
                # Otherwise, we'll skip this for now
                logger.info(f"Watchlist generation for historical dates requires script modification")
                return False
                
            except ImportError:
                logger.warning("Watchlist generation script not found")
                return False
            
        except Exception as e:
            logger.error(f"Error generating historical watchlist: {e}")
            return False
    
    def fill_watchlist_gaps(self, gaps: List[datetime], max_days: int = 7) -> int:
        """Fill missing watchlist predictions."""
        
        if not gaps:
            logger.info("No watchlist gaps to fill")
            return 0
        
        # Only fill recent gaps (generating old predictions may not be useful)
        recent_gaps = [g for g in gaps if g >= datetime.now() - timedelta(days=max_days)]
        
        if not recent_gaps:
            logger.info(f"No recent watchlist gaps (last {max_days} days)")
            return 0
        
        logger.info(f"Filling {len(recent_gaps)} watchlist gaps")
        
        successful = 0
        for gap_date in recent_gaps:
            if self.generate_historical_watchlist(gap_date):
                successful += 1
        
        logger.info(f"Successfully filled {successful}/{len(recent_gaps)} watchlist gaps")
        return successful
    
    def generate_gap_report(self) -> str:
        """Generate detailed gap analysis report."""
        
        report = []
        report.append("\n" + "=" * 70)
        report.append("DATA GAP ANALYSIS REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 70)
        
        # Summary
        total_gaps = sum(len(gaps) for gaps in self.gaps_report.values())
        report.append(f"\n📊 TOTAL GAPS FOUND: {total_gaps}")
        
        # Bhav Copy Gaps
        bhav_gaps = self.gaps_report['bhav_copy']
        if bhav_gaps:
            report.append(f"\n📈 BHAV COPY GAPS ({len(bhav_gaps)} days):")
            # Show first 10 and last 5
            if len(bhav_gaps) <= 15:
                for gap in bhav_gaps:
                    report.append(f"   - {gap.date()} ({gap.strftime('%A')})")
            else:
                for gap in bhav_gaps[:5]:
                    report.append(f"   - {gap.date()} ({gap.strftime('%A')})")
                report.append(f"   ... {len(bhav_gaps) - 10} more gaps ...")
                for gap in bhav_gaps[-5:]:
                    report.append(f"   - {gap.date()} ({gap.strftime('%A')})")
        else:
            report.append("\n✅ No bhav copy gaps found")
        
        # Watchlist Gaps
        watchlist_gaps = self.gaps_report['watchlist']
        if watchlist_gaps:
            report.append(f"\n📋 WATCHLIST GAPS ({len(watchlist_gaps)} days):")
            # Show recent gaps only
            recent = [g for g in watchlist_gaps if g >= datetime.now() - timedelta(days=14)]
            if recent:
                for gap in recent[:10]:
                    report.append(f"   - {gap.date()} ({gap.strftime('%A')})")
                if len(recent) > 10:
                    report.append(f"   ... and {len(recent) - 10} more recent gaps")
        else:
            report.append("\n✅ No watchlist gaps found")
        
        # Analysis
        if bhav_gaps:
            report.append(f"\n📊 GAP ANALYSIS:")
            
            # Find consecutive gaps (might indicate system was down)
            consecutive_gaps = []
            if len(bhav_gaps) > 1:
                current_streak = [bhav_gaps[0]]
                for i in range(1, len(bhav_gaps)):
                    if (bhav_gaps[i] - bhav_gaps[i-1]).days <= 3:  # Allow for weekends
                        current_streak.append(bhav_gaps[i])
                    else:
                        if len(current_streak) >= 3:
                            consecutive_gaps.append(current_streak)
                        current_streak = [bhav_gaps[i]]
                if len(current_streak) >= 3:
                    consecutive_gaps.append(current_streak)
            
            if consecutive_gaps:
                report.append(f"   ⚠️ Found {len(consecutive_gaps)} periods of consecutive gaps:")
                for streak in consecutive_gaps:
                    report.append(f"      {streak[0].date()} to {streak[-1].date()} "
                                f"({len(streak)} days)")
            
            # Recent vs old gaps
            recent_date = datetime.now() - timedelta(days=30)
            recent_gaps = [g for g in bhav_gaps if g >= recent_date]
            old_gaps = [g for g in bhav_gaps if g < recent_date]
            
            report.append(f"   📅 Recent gaps (last 30 days): {len(recent_gaps)}")
            report.append(f"   📅 Older gaps: {len(old_gaps)}")
        
        # Recommendations
        report.append(f"\n💡 RECOMMENDATIONS:")
        
        if len(bhav_gaps) > 0:
            recent_bhav_gaps = [g for g in bhav_gaps 
                               if g >= datetime.now() - timedelta(days=30)]
            if recent_bhav_gaps:
                report.append(f"   1. Fill {len(recent_bhav_gaps)} recent bhav_copy gaps")
                report.append(f"      Run: python gap_filler.py --fill-bhav --days 30")
        
        if len(watchlist_gaps) > 0:
            recent_wl_gaps = [g for g in watchlist_gaps 
                            if g >= datetime.now() - timedelta(days=7)]
            if recent_wl_gaps:
                report.append(f"   2. Regenerate {len(recent_wl_gaps)} recent watchlists")
                report.append(f"      Run: python gap_filler.py --fill-watchlist --days 7")
        
        if total_gaps == 0:
            report.append("   ✅ No action needed - all data is complete!")
        
        report.append("\n" + "=" * 70)
        
        return "\n".join(report)
    
    def run_gap_analysis(self, days_back: int = 30, 
                        fill_bhav: bool = False,
                        fill_watchlist: bool = False):
        """Run complete gap analysis and optionally fill gaps."""
        
        logger.info(f"Running gap analysis for last {days_back} days")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Find all gaps
        bhav_gaps = self.find_bhav_copy_gaps(start_date, end_date)
        watchlist_gaps = self.find_watchlist_gaps(start_date, end_date)
        
        # Generate report
        report = self.generate_gap_report()
        print(report)
        
        # Save report to file
        report_file = f"gap_report_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        with open(report_file, 'w') as f:
            f.write(report)
        logger.info(f"Report saved to {report_file}")
        
        # Fill gaps if requested
        if fill_bhav and bhav_gaps:
            self.fill_bhav_copy_gaps(bhav_gaps, max_days=days_back)
        
        if fill_watchlist and watchlist_gaps:
            self.fill_watchlist_gaps(watchlist_gaps, max_days=min(7, days_back))
        
        return len(bhav_gaps), len(watchlist_gaps)
    
    def cleanup(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main function for gap filler."""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Gap Filler')
    parser.add_argument('--days', type=int, default=30,
                       help='Number of days to look back (default: 30)')
    parser.add_argument('--fill-bhav', action='store_true',
                       help='Fill missing bhav copy data')
    parser.add_argument('--fill-watchlist', action='store_true',
                       help='Fill missing watchlist predictions')
    parser.add_argument('--fill-all', action='store_true',
                       help='Fill all gaps found')
    
    args = parser.parse_args()
    
    # Override individual flags if fill-all is set
    if args.fill_all:
        args.fill_bhav = True
        args.fill_watchlist = True
    
    logger.info("Starting Data Gap Filler...")
    
    try:
        filler = DataGapFiller()
        
        bhav_gaps, watchlist_gaps = filler.run_gap_analysis(
            days_back=args.days,
            fill_bhav=args.fill_bhav,
            fill_watchlist=args.fill_watchlist
        )
        
        filler.cleanup()
        
        # Print summary
        print(f"\n✅ Gap analysis completed:")
        print(f"   Bhav copy gaps: {bhav_gaps}")
        print(f"   Watchlist gaps: {watchlist_gaps}")
        
        if args.fill_bhav or args.fill_watchlist:
            print(f"   Gaps filled: Check logs for details")
        
    except Exception as e:
        logger.error(f"Error in gap filler: {e}")
        raise


if __name__ == "__main__":
    main()
