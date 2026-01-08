#!/usr/bin/env python3
"""
Paper Trading Tracker - Patched for Column Compatibility
Fixed to work with both 'direction' and 'predicted_direction' columns.

FIXES APPLIED:
- Added DROP TABLE IF EXISTS to recreate tables with correct schema
- Convert Decimal to float before arithmetic operations
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta
import logging

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


def to_float(value):
    """
    Convert Decimal or other numeric types to Python float.
    Returns 0.0 if conversion fails.
    """
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class PaperTradingTracker:
    """Simple paper trading tracker for watchlist predictions."""
    
    def __init__(self, recreate_tables=False):
        """
        Initialize tracker.
        
        Args:
            recreate_tables: If True, drop and recreate tables (use if schema changed)
        """
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.setup_tables(recreate=recreate_tables)
    
    def setup_tables(self, recreate=False):
        """Create paper trading tables if they don't exist."""
        
        if recreate:
            logger.warning("Recreating paper trading tables (existing data will be lost)")
            drop_queries = [
                "DROP TABLE IF EXISTS trading_performance CASCADE;",
                "DROP TABLE IF EXISTS paper_trades CASCADE;"
            ]
            with self.conn.cursor() as cur:
                for query in drop_queries:
                    cur.execute(query)
            self.conn.commit()
        
        queries = [
            """
            CREATE TABLE IF NOT EXISTS paper_trades (
                id SERIAL PRIMARY KEY,
                entry_date DATE NOT NULL,
                sc_code VARCHAR(20) NOT NULL,
                sc_name VARCHAR(200),
                
                -- Entry details
                direction VARCHAR(10) DEFAULT 'BUY',
                entry_price DECIMAL(15,2) NOT NULL,
                quantity INTEGER DEFAULT 100,
                position_value DECIMAL(15,2),
                
                -- Exit details
                exit_date DATE,
                exit_price DECIMAL(15,2),
                exit_reason VARCHAR(50),
                
                -- Performance
                pnl_amount DECIMAL(15,2),
                pnl_percent DECIMAL(10,2),
                holding_days INTEGER,
                
                -- Status
                status VARCHAR(20) DEFAULT 'OPEN',
                
                -- Metadata
                confidence_score DECIMAL(5,2),
                strategy_type VARCHAR(50),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            
            """
            CREATE TABLE IF NOT EXISTS trading_performance (
                id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                total_trades INTEGER,
                open_positions INTEGER,
                closed_today INTEGER,
                
                -- Daily performance
                daily_pnl DECIMAL(15,2),
                daily_pnl_percent DECIMAL(10,2),
                
                -- Cumulative performance
                total_winners INTEGER,
                total_losers INTEGER,
                win_rate DECIMAL(5,2),
                avg_win DECIMAL(10,2),
                avg_loss DECIMAL(10,2),
                profit_factor DECIMAL(10,2),
                
                -- Capital
                starting_capital DECIMAL(15,2) DEFAULT 1000000,
                current_capital DECIMAL(15,2),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades(status);",
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_date ON paper_trades(entry_date);",
            "CREATE INDEX IF NOT EXISTS idx_paper_trades_code ON paper_trades(sc_code);"
        ]
        
        with self.conn.cursor() as cur:
            for query in queries:
                try:
                    cur.execute(query)
                except Exception as e:
                    logger.warning(f"Table creation warning: {e}")
        self.conn.commit()
        logger.info("Paper trading tables ready")
    
    def check_table_schema(self):
        """Check if table has required columns, recreate if not."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = 'paper_trades' AND column_name = 'entry_date'
                """)
                if not cur.fetchone():
                    logger.warning("paper_trades table missing entry_date column, recreating...")
                    self.setup_tables(recreate=True)
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking schema: {e}")
            return False
    
    def _get_direction(self, row):
        """
        Get direction from row, handling different column names.
        COMPATIBILITY FIX: Works with both 'direction' and 'predicted_direction'
        """
        # Check various column names in order of preference
        for col in ['direction', 'predicted_direction', 'signal', 'trade_direction']:
            if col in row.index and pd.notna(row[col]) and row[col] != '':
                direction_val = str(row[col]).upper()
                
                # Normalize to BUY/SELL
                if direction_val in ['BULLISH', 'BUY', 'LONG', 'UP']:
                    return 'BUY'
                elif direction_val in ['BEARISH', 'SELL', 'SHORT', 'DOWN']:
                    return 'SELL'
                elif direction_val in ['NEUTRAL', 'NEUTRAL/WAIT', 'WAIT']:
                    return 'HOLD'  # Skip these
        
        # Fallback: infer from momentum or RSI
        if 'momentum_3d' in row.index and pd.notna(row['momentum_3d']):
            return 'BUY' if row['momentum_3d'] > 0 else 'SELL'
        
        if 'rsi' in row.index and pd.notna(row['rsi']):
            if row['rsi'] > 70:
                return 'SELL'  # Overbought
            elif row['rsi'] < 30:
                return 'BUY'   # Oversold
        
        # Default to BUY for momentum strategy
        return 'BUY'
    
    def _get_score(self, row):
        """
        Get confidence score from row, handling different column names.
        COMPATIBILITY FIX: Works with various score column names
        """
        for col in ['options_score', 'combined_score', 'combined_ml_score', 
                    'confidence_score', 'score', 'momentum_score']:
            if col in row.index and pd.notna(row[col]):
                return to_float(row[col])
        return 50.0  # Default
    
    def enter_trades_from_watchlist(self, date=None):
        """Enter paper trades from today's watchlist."""
        
        # First check if table schema is correct
        self.check_table_schema()
        
        if date is None:
            date = datetime.now().date()
        
        # Look for watchlist file
        watchlist_files = [
            f"enhanced_watchlist_{date.strftime('%Y%m%d')}.csv",
            f"options_watchlist_{date.strftime('%Y%m%d')}.csv",
            f"watchlist_{date.strftime('%Y%m%d')}.csv"
        ]
        
        watchlist_file = None
        for file in watchlist_files:
            if os.path.exists(file):
                watchlist_file = file
                break
        
        if not watchlist_file:
            logger.warning(f"No watchlist file found for {date}")
            return 0
        
        logger.info(f"Reading watchlist from {watchlist_file}")
        df = pd.read_csv(watchlist_file)
        
        if df.empty:
            logger.warning("Watchlist is empty")
            return 0
        
        # Standardize column names (handle different formats)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # COMPATIBILITY FIX: Sort by available score column
        score_col = None
        for col in ['options_score', 'combined_score', 'combined_ml_score', 'score']:
            if col in df.columns:
                score_col = col
                break
        
        if score_col:
            df = df.nlargest(10, score_col)
        else:
            df = df.head(10)
        
        trades_entered = 0
        
        for idx, row in df.iterrows():
            try:
                # Get stock details
                sc_code = row.get('sc_code', '')
                sc_name = row.get('sc_name', '')
                entry_price = to_float(row.get('close_price', row.get('close', 0)))
                
                if not sc_code or entry_price <= 0:
                    continue
                
                # COMPATIBILITY FIX: Get direction using helper
                direction = self._get_direction(row)
                
                if direction == 'HOLD':
                    logger.debug(f"Skipping {sc_code} - NEUTRAL/WAIT signal")
                    continue
                
                # Check if already have an open position
                check_query = """
                SELECT id FROM paper_trades 
                WHERE sc_code = %s AND status = 'OPEN'
                """
                with self.conn.cursor() as cur:
                    cur.execute(check_query, (sc_code,))
                    if cur.fetchone():
                        logger.debug(f"Already have open position in {sc_code}")
                        continue
                
                # Calculate position size (equal weight, 10 positions = 10% each)
                position_value = 100000  # 1 Lakh per position
                quantity = int(position_value / entry_price)
                
                # COMPATIBILITY FIX: Get score using helper
                confidence_score = self._get_score(row)
                strategy_type = row.get('strategy_type', 'MOMENTUM')
                
                # Insert paper trade
                insert_query = """
                INSERT INTO paper_trades (
                    entry_date, sc_code, sc_name, direction,
                    entry_price, quantity, position_value,
                    confidence_score, strategy_type, status
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 'OPEN'
                )
                """
                
                with self.conn.cursor() as cur:
                    cur.execute(insert_query, (
                        date,
                        sc_code,
                        sc_name,
                        direction,
                        entry_price,
                        quantity,
                        position_value,
                        confidence_score,
                        strategy_type
                    ))
                
                trades_entered += 1
                logger.info(f"Entered {direction} trade: {sc_code} @ {entry_price:.2f}")
                
            except Exception as e:
                logger.error(f"Error entering trade for {row.get('sc_code', 'unknown')}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Entered {trades_entered} new paper trades")
        return trades_entered
    
    def update_open_trades(self):
        """Update all open trades with current prices and close if needed."""
        
        # Get all open trades
        query = """
        SELECT id, sc_code, entry_date, entry_price, direction, quantity
        FROM paper_trades
        WHERE status = 'OPEN'
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            open_trades = cur.fetchall()
        
        if not open_trades:
            logger.info("No open trades to update")
            return
        
        logger.info(f"Updating {len(open_trades)} open trades")
        closed_count = 0
        
        for trade in open_trades:
            try:
                # Get current price from bhav_copy
                price_query = """
                SELECT close_price, high_price, low_price
                FROM bhav_copy
                WHERE sc_code = %s
                    AND trading_date = (
                        SELECT MAX(trading_date) 
                        FROM bhav_copy 
                        WHERE sc_code = %s
                    )
                """
                
                with self.conn.cursor() as cur:
                    cur.execute(price_query, (trade['sc_code'], trade['sc_code']))
                    price_data = cur.fetchone()
                
                if not price_data:
                    logger.warning(f"No price data for {trade['sc_code']}")
                    continue
                
                # FIX: Convert Decimal to float
                current_price = to_float(price_data[0])
                high_price = to_float(price_data[1])
                low_price = to_float(price_data[2])
                entry_price = to_float(trade['entry_price'])
                quantity = int(trade['quantity']) if trade['quantity'] else 100
                
                if entry_price <= 0:
                    continue
                
                # Calculate P&L
                if trade['direction'] == 'BUY':
                    pnl_percent = ((current_price - entry_price) / entry_price) * 100
                    pnl_amount = (current_price - entry_price) * quantity
                else:  # SELL
                    pnl_percent = ((entry_price - current_price) / entry_price) * 100
                    pnl_amount = (entry_price - current_price) * quantity
                
                # Calculate holding days
                holding_days = (datetime.now().date() - trade['entry_date']).days
                
                # Exit conditions
                should_close = False
                exit_reason = None
                
                # 1. Stop loss hit (-2%)
                if pnl_percent <= -2:
                    should_close = True
                    exit_reason = 'STOP_LOSS'
                
                # 2. Target hit (+3%)
                elif pnl_percent >= 3:
                    should_close = True
                    exit_reason = 'TARGET'
                
                # 3. Time exit (5 days)
                elif holding_days >= 5:
                    should_close = True
                    exit_reason = 'TIME_EXIT'
                
                # 4. Trailing stop (if profit > 2% and pulls back to 1%)
                elif pnl_percent > 1 and pnl_percent < 1.5 and holding_days >= 2:
                    if trade['direction'] == 'BUY' and high_price > current_price * 1.015:
                        should_close = True
                        exit_reason = 'TRAILING_STOP'
                
                if should_close:
                    # Close the trade
                    close_query = """
                    UPDATE paper_trades
                    SET exit_date = %s,
                        exit_price = %s,
                        exit_reason = %s,
                        pnl_amount = %s,
                        pnl_percent = %s,
                        holding_days = %s,
                        status = 'CLOSED'
                    WHERE id = %s
                    """
                    
                    with self.conn.cursor() as cur:
                        cur.execute(close_query, (
                            datetime.now().date(),
                            current_price,
                            exit_reason,
                            pnl_amount,
                            pnl_percent,
                            holding_days,
                            trade['id']
                        ))
                    
                    closed_count += 1
                    logger.info(f"Closed {trade['sc_code']}: {pnl_percent:.2f}% ({exit_reason})")
                
            except Exception as e:
                logger.error(f"Error updating trade {trade['id']}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Closed {closed_count} trades")
    
    def calculate_daily_performance(self):
        """Calculate and store daily performance metrics."""
        
        today = datetime.now().date()
        
        # Get all trades
        query = """
        SELECT * FROM paper_trades
        """
        
        df = pd.read_sql_query(query, self.conn)
        
        if df.empty:
            logger.info("No trades to analyze")
            return
        
        # Basic counts
        total_trades = len(df)
        open_positions = len(df[df['status'] == 'OPEN'])
        closed_today = len(df[(df['status'] == 'CLOSED') & (df['exit_date'] == today)])
        
        # Closed trades analysis
        closed = df[df['status'] == 'CLOSED']
        
        if len(closed) > 0:
            winners = closed[closed['pnl_percent'] > 0]
            losers = closed[closed['pnl_percent'] <= 0]
            
            win_rate = len(winners) / len(closed) * 100
            avg_win = to_float(winners['pnl_percent'].mean()) if len(winners) > 0 else 0
            avg_loss = to_float(losers['pnl_percent'].mean()) if len(losers) > 0 else 0
            
            total_pnl = to_float(closed['pnl_amount'].sum())
            
            # Profit factor
            gross_profit = to_float(winners['pnl_amount'].sum()) if len(winners) > 0 else 0
            gross_loss = abs(to_float(losers['pnl_amount'].sum())) if len(losers) > 0 else 1
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
            
        else:
            win_rate = 0
            avg_win = 0
            avg_loss = 0
            total_pnl = 0
            profit_factor = 0
        
        # Calculate current capital
        starting_capital = 1000000
        current_capital = starting_capital + total_pnl
        
        # Save performance
        upsert_query = """
        INSERT INTO trading_performance (
            date, total_trades, open_positions, closed_today,
            daily_pnl, total_winners, total_losers, win_rate,
            avg_win, avg_loss, profit_factor, 
            starting_capital, current_capital
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (date) DO UPDATE SET
            total_trades = EXCLUDED.total_trades,
            open_positions = EXCLUDED.open_positions,
            current_capital = EXCLUDED.current_capital
        """
        
        with self.conn.cursor() as cur:
            cur.execute(upsert_query, (
                today,
                total_trades,
                open_positions,
                closed_today,
                total_pnl,
                len(closed[closed['pnl_percent'] > 0]) if len(closed) > 0 else 0,
                len(closed[closed['pnl_percent'] <= 0]) if len(closed) > 0 else 0,
                win_rate,
                avg_win,
                avg_loss,
                profit_factor,
                starting_capital,
                current_capital
            ))
        
        self.conn.commit()
        logger.info(f"Performance updated: {len(closed)} closed trades, "
                   f"Win rate: {win_rate:.1f}%, P&L: Rs {total_pnl:,.0f}")
    
    def generate_report(self, days: int = 30):
        """Generate performance report."""
        
        print("\n" + "=" * 60)
        print("PAPER TRADING PERFORMANCE REPORT")
        print(f"Period: Last {days} days | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        
        # Get recent trades
        trades_query = """
        SELECT * FROM paper_trades
        WHERE entry_date >= %s
        ORDER BY entry_date DESC, exit_date DESC
        """
        
        trades_df = pd.read_sql_query(trades_query, self.conn, 
                                     params=(datetime.now().date() - timedelta(days=days),))
        
        if trades_df.empty:
            print("\nNo trades found in the period")
            return
        
        # Overall stats
        closed = trades_df[trades_df['status'] == 'CLOSED']
        open_trades = trades_df[trades_df['status'] == 'OPEN']
        
        print("\nOVERALL STATISTICS:")
        print(f"Total Trades: {len(trades_df)}")
        print(f"Open Positions: {len(open_trades)}")
        print(f"Closed Trades: {len(closed)}")
        
        if len(closed) > 0:
            winners = closed[closed['pnl_percent'] > 0]
            losers = closed[closed['pnl_percent'] <= 0]
            
            print(f"\nWinners: {len(winners)} ({len(winners)/len(closed)*100:.1f}%)")
            print(f"Losers: {len(losers)} ({len(losers)/len(closed)*100:.1f}%)")
            
            print(f"\nPERFORMANCE:")
            print(f"Total P&L: Rs {to_float(closed['pnl_amount'].sum()):,.0f}")
            print(f"Avg P&L per trade: Rs {to_float(closed['pnl_amount'].mean()):,.0f}")
            print(f"Best Trade: {to_float(closed['pnl_percent'].max()):.2f}%")
            print(f"Worst Trade: {to_float(closed['pnl_percent'].min()):.2f}%")
            print(f"Avg Winner: {to_float(winners['pnl_percent'].mean()):.2f}%" if len(winners) > 0 else "Avg Winner: N/A")
            print(f"Avg Loser: {to_float(losers['pnl_percent'].mean()):.2f}%" if len(losers) > 0 else "Avg Loser: N/A")
            
            if len(losers) > 0 and to_float(losers['pnl_amount'].sum()) != 0:
                profit_factor = abs(to_float(winners['pnl_amount'].sum()) / to_float(losers['pnl_amount'].sum()))
                print(f"Profit Factor: {profit_factor:.2f}")
            
            print(f"Avg Holding Days: {to_float(closed['holding_days'].mean()):.1f}")
        
        # Current open positions
        if len(open_trades) > 0:
            print("\nOPEN POSITIONS:")
            for _, trade in open_trades.head(10).iterrows():
                # Get current price for P&L
                price_query = """
                SELECT close_price FROM bhav_copy 
                WHERE sc_code = %s 
                ORDER BY trading_date DESC LIMIT 1
                """
                with self.conn.cursor() as cur:
                    cur.execute(price_query, (trade['sc_code'],))
                    result = cur.fetchone()
                    if result:
                        current_price = to_float(result[0])
                        entry_price = to_float(trade['entry_price'])
                        
                        if entry_price > 0:
                            if trade['direction'] == 'BUY':
                                current_pnl = ((current_price - entry_price) / entry_price) * 100
                            else:
                                current_pnl = ((entry_price - current_price) / entry_price) * 100
                            
                            days_held = (datetime.now().date() - trade['entry_date']).days
                            print(f"  {trade['sc_code']}: {current_pnl:+.2f}% (Day {days_held})")
        
        print("\n" + "=" * 60)
    
    def cleanup(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main function to run paper trading tracker."""
    
    logger.info("Starting Paper Trading Tracker...")
    
    try:
        # First run with recreate_tables=True to fix schema if needed
        tracker = PaperTradingTracker(recreate_tables=False)
        
        # Check and fix schema if needed
        if tracker.check_table_schema():
            logger.info("Table schema was fixed, continuing...")
        
        # Enter new trades from today's watchlist
        tracker.enter_trades_from_watchlist()
        
        # Update all open positions
        tracker.update_open_trades()
        
        # Calculate performance
        tracker.calculate_daily_performance()
        
        # Generate report
        tracker.generate_report()
        
        tracker.cleanup()
        
        logger.info("Paper trading update completed")
        
    except Exception as e:
        logger.error(f"Error in paper trading: {e}")
        raise


if __name__ == "__main__":
    main()
