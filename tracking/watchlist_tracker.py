#!/usr/bin/env python3
"""
Watchlist Tracker - Patched for Column Compatibility
Fixed to work with both 'direction' and 'predicted_direction' columns.
Also handles 'options_score' vs 'combined_score'.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import pandas as pd
import numpy as np
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


class WatchlistTracker:
    """Track and evaluate watchlist predictions."""
    
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.setup_tables()
    
    def setup_tables(self):
        """Create prediction tracking tables."""
        
        queries = [
            """
            CREATE TABLE IF NOT EXISTS watchlist_predictions (
                id SERIAL PRIMARY KEY,
                prediction_date DATE NOT NULL,
                sc_code VARCHAR(20) NOT NULL,
                sc_name VARCHAR(200),
                
                -- Prediction details
                predicted_direction VARCHAR(20) DEFAULT 'BULLISH',
                entry_price DECIMAL(15,2),
                target_price DECIMAL(15,2),
                stop_loss DECIMAL(15,2),
                
                -- Scores and indicators
                combined_score DECIMAL(5,2),
                options_score DECIMAL(5,2),
                momentum_score DECIMAL(5,2),
                volume_ratio DECIMAL(10,2),
                rsi DECIMAL(5,2),
                
                -- Strategy
                strategy_type VARCHAR(50),
                
                -- Additional fields from watchlist
                momentum_3d DECIMAL(10,2),
                atr_pct DECIMAL(10,2),
                turnover_cr DECIMAL(15,2),
                price_strength DECIMAL(5,2),
                strike_suggestion VARCHAR(100),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(prediction_date, sc_code)
            );
            """,
            
            """
            CREATE TABLE IF NOT EXISTS prediction_results (
                id SERIAL PRIMARY KEY,
                prediction_id INTEGER REFERENCES watchlist_predictions(id),
                evaluation_date DATE,
                days_after INTEGER,
                
                -- Actual performance
                actual_open DECIMAL(15,2),
                actual_high DECIMAL(15,2),
                actual_low DECIMAL(15,2),
                actual_close DECIMAL(15,2),
                actual_volume BIGINT,
                
                -- Results
                direction_correct BOOLEAN,
                target_hit BOOLEAN,
                stop_hit BOOLEAN,
                max_gain_pct DECIMAL(10,2),
                max_loss_pct DECIMAL(10,2),
                close_gain_pct DECIMAL(10,2),
                
                UNIQUE(prediction_id, days_after)
            );
            """,
            
            """
            CREATE TABLE IF NOT EXISTS accuracy_summary (
                id SERIAL PRIMARY KEY,
                summary_date DATE UNIQUE,
                period_days INTEGER DEFAULT 30,
                
                -- Overall metrics
                total_predictions INTEGER,
                total_evaluated INTEGER,
                
                -- Direction accuracy
                direction_correct_1d INTEGER,
                direction_correct_3d INTEGER,
                direction_correct_5d INTEGER,
                direction_accuracy_1d DECIMAL(5,2),
                direction_accuracy_3d DECIMAL(5,2),
                direction_accuracy_5d DECIMAL(5,2),
                
                -- Target/Stop metrics
                targets_hit_pct DECIMAL(5,2),
                stops_hit_pct DECIMAL(5,2),
                
                -- Performance by score
                high_score_accuracy DECIMAL(5,2),
                medium_score_accuracy DECIMAL(5,2),
                low_score_accuracy DECIMAL(5,2),
                
                -- Best performers
                best_strategy VARCHAR(50),
                best_strategy_accuracy DECIMAL(5,2),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            
            """
            CREATE INDEX IF NOT EXISTS idx_predictions_date ON watchlist_predictions(prediction_date);
            CREATE INDEX IF NOT EXISTS idx_predictions_code ON watchlist_predictions(sc_code);
            CREATE INDEX IF NOT EXISTS idx_results_date ON prediction_results(evaluation_date);
            """
        ]
        
        with self.conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
        self.conn.commit()
        logger.info("Watchlist tracking tables ready")
    
    def _get_direction(self, row):
        """
        Get direction from row, handling different column names.
        COMPATIBILITY FIX: Works with 'direction', 'predicted_direction', etc.
        """
        # Check various column names
        for col in ['direction', 'predicted_direction', 'signal']:
            if col in row.index and pd.notna(row[col]) and row[col] != '':
                direction_val = str(row[col]).upper()
                
                # Normalize values
                if direction_val in ['BULLISH', 'BUY', 'LONG', 'UP']:
                    return 'BULLISH'
                elif direction_val in ['BEARISH', 'SELL', 'SHORT', 'DOWN']:
                    return 'BEARISH'
                elif direction_val in ['NEUTRAL', 'NEUTRAL/WAIT', 'WAIT']:
                    return 'NEUTRAL'
        
        # Fallback: infer from RSI if available
        if 'rsi' in row.index and pd.notna(row['rsi']):
            if row['rsi'] > 70:
                return 'BEARISH'  # Overbought
            elif row['rsi'] < 30:
                return 'BULLISH'  # Oversold
        
        # Fallback: infer from momentum
        if 'momentum_3d' in row.index and pd.notna(row['momentum_3d']):
            return 'BULLISH' if row['momentum_3d'] > 0 else 'BEARISH'
        
        # Default
        return 'BULLISH'
    
    def _get_score(self, row):
        """
        Get score from row, handling different column names.
        COMPATIBILITY FIX: Works with various score columns.
        """
        for col in ['options_score', 'combined_score', 'combined_ml_score', 
                    'confidence_score', 'score']:
            if col in row.index and pd.notna(row[col]):
                return float(row[col])
        return 50.0
    
    def save_daily_predictions(self, date=None):
        """Save predictions from today's watchlist file."""
        
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
        
        logger.info(f"Saving predictions from {watchlist_file}")
        df = pd.read_csv(watchlist_file)
        
        if df.empty:
            logger.warning("Watchlist is empty")
            return 0
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        saved_count = 0
        
        for idx, row in df.iterrows():
            try:
                sc_code = row.get('sc_code', '')
                if not sc_code:
                    continue
                
                # COMPATIBILITY FIX: Get direction using helper
                predicted_direction = self._get_direction(row)
                
                # Calculate target and stop loss if not provided
                entry_price = float(row.get('close_price', row.get('close', 0)))
                
                if entry_price <= 0:
                    continue
                
                if predicted_direction == 'BULLISH':
                    target_price = entry_price * 1.03  # 3% target
                    stop_loss = entry_price * 0.98     # 2% stop loss
                elif predicted_direction == 'BEARISH':
                    target_price = entry_price * 0.97  # 3% target for short
                    stop_loss = entry_price * 1.02     # 2% stop loss
                else:  # NEUTRAL
                    target_price = entry_price * 1.02
                    stop_loss = entry_price * 0.98
                
                # COMPATIBILITY FIX: Get score using helper
                combined_score = self._get_score(row)
                
                # Insert or update prediction
                upsert_query = """
                INSERT INTO watchlist_predictions (
                    prediction_date, sc_code, sc_name,
                    predicted_direction, entry_price, target_price, stop_loss,
                    combined_score, options_score, momentum_score, volume_ratio, rsi,
                    strategy_type, momentum_3d, atr_pct, turnover_cr, 
                    price_strength, strike_suggestion
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (prediction_date, sc_code) DO UPDATE SET
                    predicted_direction = EXCLUDED.predicted_direction,
                    entry_price = EXCLUDED.entry_price,
                    target_price = EXCLUDED.target_price,
                    stop_loss = EXCLUDED.stop_loss,
                    combined_score = EXCLUDED.combined_score,
                    options_score = EXCLUDED.options_score
                """
                
                with self.conn.cursor() as cur:
                    cur.execute(upsert_query, (
                        date,
                        sc_code,
                        row.get('sc_name', ''),
                        predicted_direction,
                        entry_price,
                        target_price,
                        stop_loss,
                        combined_score,
                        row.get('options_score', combined_score),  # Use options_score if available
                        row.get('momentum_score', row.get('momentum_3d', 0)),
                        row.get('volume_ratio', 0),
                        row.get('rsi', 0),
                        row.get('strategy_type', 'MOMENTUM'),
                        row.get('momentum_3d', 0),
                        row.get('atr_pct', 0),
                        row.get('turnover_cr', 0),
                        row.get('price_strength', 50),
                        row.get('strike_suggestion', '')
                    ))
                
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error saving prediction for {row.get('sc_code', 'unknown')}: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Saved {saved_count} predictions")
        return saved_count
    
    def evaluate_predictions(self, evaluation_date=None):
        """Evaluate predictions made 1, 3, and 5 days ago."""
        
        if evaluation_date is None:
            evaluation_date = datetime.now().date()
        
        logger.info(f"Evaluating predictions for {evaluation_date}")
        
        # Evaluate predictions from 1, 3, and 5 days ago
        for days_back in [1, 3, 5]:
            prediction_date = evaluation_date - timedelta(days=days_back)
            
            # Skip weekends
            if prediction_date.weekday() >= 5:
                continue
            
            # Get predictions from that day
            query = """
            SELECT id, sc_code, predicted_direction, entry_price, 
                   target_price, stop_loss
            FROM watchlist_predictions
            WHERE prediction_date = %s
            """
            
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (prediction_date,))
                predictions = cur.fetchall()
            
            if not predictions:
                logger.debug(f"No predictions to evaluate for {prediction_date}")
                continue
            
            logger.info(f"Evaluating {len(predictions)} predictions from {prediction_date}")
            
            for pred in predictions:
                try:
                    # Get actual price data
                    price_query = """
                    SELECT open_price, high_price, low_price, close_price, no_of_shrs
                    FROM bhav_copy
                    WHERE sc_code = %s 
                        AND trading_date = %s
                    """
                    
                    with self.conn.cursor() as cur:
                        cur.execute(price_query, (pred['sc_code'], evaluation_date))
                        price_data = cur.fetchone()
                    
                    if not price_data:
                        continue
                    
                    actual_open, actual_high, actual_low, actual_close, actual_volume = price_data
                    
                    # Calculate results
                    entry = float(pred['entry_price'])
                    target = float(pred['target_price'])
                    stop = float(pred['stop_loss'])
                    direction = pred['predicted_direction']
                    
                    if direction == 'BULLISH':
                        close_gain_pct = (actual_close - entry) / entry * 100
                        max_gain_pct = (actual_high - entry) / entry * 100
                        max_loss_pct = (actual_low - entry) / entry * 100
                        direction_correct = actual_close > entry
                        target_hit = actual_high >= target
                        stop_hit = actual_low <= stop
                    else:  # BEARISH
                        close_gain_pct = (entry - actual_close) / entry * 100
                        max_gain_pct = (entry - actual_low) / entry * 100
                        max_loss_pct = (entry - actual_high) / entry * 100
                        direction_correct = actual_close < entry
                        target_hit = actual_low <= target
                        stop_hit = actual_high >= stop
                    
                    # Insert result
                    result_query = """
                    INSERT INTO prediction_results (
                        prediction_id, evaluation_date, days_after,
                        actual_open, actual_high, actual_low, actual_close, actual_volume,
                        direction_correct, target_hit, stop_hit,
                        max_gain_pct, max_loss_pct, close_gain_pct
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (prediction_id, days_after) DO UPDATE SET
                        direction_correct = EXCLUDED.direction_correct,
                        close_gain_pct = EXCLUDED.close_gain_pct
                    """
                    
                    with self.conn.cursor() as cur:
                        cur.execute(result_query, (
                            pred['id'],
                            evaluation_date,
                            days_back,
                            actual_open, actual_high, actual_low, actual_close, actual_volume,
                            direction_correct, target_hit, stop_hit,
                            max_gain_pct, max_loss_pct, close_gain_pct
                        ))
                    
                except Exception as e:
                    logger.error(f"Error evaluating {pred['sc_code']}: {e}")
                    continue
            
            self.conn.commit()
        
        logger.info("Evaluation completed")
    
    def calculate_accuracy_summary(self, date=None):
        """Calculate accuracy summary for the last 30 days."""
        
        if date is None:
            date = datetime.now().date()
        
        logger.info("Calculating accuracy summary...")
        
        # Get all predictions with results
        summary_query = """
        SELECT 
            p.id,
            p.prediction_date,
            p.sc_code,
            p.predicted_direction,
            COALESCE(p.combined_score, p.options_score, 50) as combined_score,
            p.strategy_type,
            r1.direction_correct as correct_1d,
            r1.target_hit as target_1d,
            r1.stop_hit as stop_1d,
            r3.direction_correct as correct_3d,
            r5.direction_correct as correct_5d
        FROM watchlist_predictions p
        LEFT JOIN prediction_results r1 ON p.id = r1.prediction_id AND r1.days_after = 1
        LEFT JOIN prediction_results r3 ON p.id = r3.prediction_id AND r3.days_after = 3
        LEFT JOIN prediction_results r5 ON p.id = r5.prediction_id AND r5.days_after = 5
        WHERE p.prediction_date >= %s
        """
        
        df = pd.read_sql_query(summary_query, self.conn, 
                              params=(date - timedelta(days=30),))
        
        if df.empty:
            logger.warning("No predictions to summarize")
            return
        
        # Calculate metrics
        total_predictions = len(df)
        
        # Direction accuracy by timeframe
        df_1d = df.dropna(subset=['correct_1d'])
        df_3d = df.dropna(subset=['correct_3d'])
        df_5d = df.dropna(subset=['correct_5d'])
        
        accuracy_1d = (df_1d['correct_1d'].sum() / len(df_1d) * 100) if len(df_1d) > 0 else 0
        accuracy_3d = (df_3d['correct_3d'].sum() / len(df_3d) * 100) if len(df_3d) > 0 else 0
        accuracy_5d = (df_5d['correct_5d'].sum() / len(df_5d) * 100) if len(df_5d) > 0 else 0
        
        # Target/Stop metrics
        df_targets = df.dropna(subset=['target_1d', 'stop_1d'])
        targets_hit = (df_targets['target_1d'].sum() / len(df_targets) * 100) if len(df_targets) > 0 else 0
        stops_hit = (df_targets['stop_1d'].sum() / len(df_targets) * 100) if len(df_targets) > 0 else 0
        
        # Accuracy by score buckets
        high_score = df[(df['combined_score'] > 70) & df['correct_1d'].notna()]
        med_score = df[(df['combined_score'].between(50, 70)) & df['correct_1d'].notna()]
        low_score = df[(df['combined_score'] < 50) & df['correct_1d'].notna()]
        
        high_acc = (high_score['correct_1d'].mean() * 100) if len(high_score) > 0 else 0
        med_acc = (med_score['correct_1d'].mean() * 100) if len(med_score) > 0 else 0
        low_acc = (low_score['correct_1d'].mean() * 100) if len(low_score) > 0 else 0
        
        # Best strategy
        strategy_df = df[df['correct_1d'].notna() & df['strategy_type'].notna()]
        if len(strategy_df) > 0:
            strategy_acc = strategy_df.groupby('strategy_type')['correct_1d'].mean() * 100
            best_strategy = strategy_acc.idxmax() if not strategy_acc.empty else 'UNKNOWN'
            best_strategy_acc = strategy_acc.max() if not strategy_acc.empty else 0
        else:
            best_strategy = 'UNKNOWN'
            best_strategy_acc = 0
        
        # Save summary
        upsert_query = """
        INSERT INTO accuracy_summary (
            summary_date, period_days, total_predictions, total_evaluated,
            direction_correct_1d, direction_correct_3d, direction_correct_5d,
            direction_accuracy_1d, direction_accuracy_3d, direction_accuracy_5d,
            targets_hit_pct, stops_hit_pct,
            high_score_accuracy, medium_score_accuracy, low_score_accuracy,
            best_strategy, best_strategy_accuracy
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (summary_date) DO UPDATE SET
            total_predictions = EXCLUDED.total_predictions,
            direction_accuracy_1d = EXCLUDED.direction_accuracy_1d,
            direction_accuracy_3d = EXCLUDED.direction_accuracy_3d,
            direction_accuracy_5d = EXCLUDED.direction_accuracy_5d
        """
        
        with self.conn.cursor() as cur:
            cur.execute(upsert_query, (
                date, 30, total_predictions, len(df_1d),
                int(df_1d['correct_1d'].sum()) if len(df_1d) > 0 else 0,
                int(df_3d['correct_3d'].sum()) if len(df_3d) > 0 else 0,
                int(df_5d['correct_5d'].sum()) if len(df_5d) > 0 else 0,
                accuracy_1d, accuracy_3d, accuracy_5d,
                targets_hit, stops_hit,
                high_acc, med_acc, low_acc,
                best_strategy, best_strategy_acc
            ))
        
        self.conn.commit()
        logger.info(f"Summary saved: 1D accuracy={accuracy_1d:.1f}%, "
                   f"3D={accuracy_3d:.1f}%, 5D={accuracy_5d:.1f}%")
    
    def generate_accuracy_report(self, days=30):
        """Generate detailed accuracy report."""
        
        print("\n" + "=" * 60)
        print("PREDICTION ACCURACY REPORT")
        print(f"Last {days} days | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 60)
        
        # Get latest summary
        summary_query = """
        SELECT * FROM accuracy_summary
        ORDER BY summary_date DESC
        LIMIT 1
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(summary_query)
            summary = cur.fetchone()
        
        if not summary:
            print("\nNo accuracy data available yet")
            return
        
        print(f"\nOVERALL ACCURACY (Last 30 days):")
        print(f"Total Predictions: {summary['total_predictions']}")
        print(f"Total Evaluated: {summary['total_evaluated']}")
        
        print(f"\nDIRECTION ACCURACY BY TIMEFRAME:")
        print(f"1-Day: {summary['direction_accuracy_1d']:.1f}% "
              f"({summary['direction_correct_1d']}/{summary['total_evaluated']})")
        print(f"3-Day: {summary['direction_accuracy_3d']:.1f}%")
        print(f"5-Day: {summary['direction_accuracy_5d']:.1f}%")
        
        print(f"\nTARGET & STOP LOSS:")
        print(f"Targets Hit: {summary['targets_hit_pct']:.1f}%")
        print(f"Stops Hit: {summary['stops_hit_pct']:.1f}%")
        
        print(f"\nACCURACY BY CONFIDENCE SCORE:")
        print(f"High Score (>70): {summary['high_score_accuracy']:.1f}%")
        print(f"Medium Score (50-70): {summary['medium_score_accuracy']:.1f}%")
        print(f"Low Score (<50): {summary['low_score_accuracy']:.1f}%")
        
        print(f"\nBEST STRATEGY:")
        print(f"{summary['best_strategy']}: {summary['best_strategy_accuracy']:.1f}% accuracy")
        
        # Get recent predictions performance
        recent_query = """
        SELECT p.sc_code, p.predicted_direction, p.entry_price,
               r.close_gain_pct, r.direction_correct, r.target_hit
        FROM watchlist_predictions p
        JOIN prediction_results r ON p.id = r.prediction_id
        WHERE r.days_after = 1 
            AND p.prediction_date >= %s
        ORDER BY p.prediction_date DESC
        LIMIT 20
        """
        
        recent_df = pd.read_sql_query(recent_query, self.conn,
                                     params=(datetime.now().date() - timedelta(days=7),))
        
        if not recent_df.empty:
            print(f"\nRECENT PREDICTIONS (Last 7 days):")
            correct = recent_df['direction_correct'].sum()
            total = len(recent_df)
            print(f"Recent Accuracy: {correct}/{total} = {correct/total*100:.1f}%")
            
            # Best recent predictions
            best = recent_df.nlargest(5, 'close_gain_pct')
            if not best.empty:
                print(f"\nBest Recent Calls:")
                for _, row in best.iterrows():
                    symbol = "[OK]" if row['direction_correct'] else "[X]"
                    print(f"  {symbol} {row['sc_code']}: {row['close_gain_pct']:.2f}%")
        
        # Statistical significance
        if summary['total_evaluated'] and summary['total_evaluated'] >= 30:
            expected = 0.5
            observed = summary['direction_accuracy_1d'] / 100
            n = summary['total_evaluated']
            
            se = np.sqrt(expected * (1 - expected) / n)
            z_score = (observed - expected) / se if se > 0 else 0
            
            print(f"\nSTATISTICAL SIGNIFICANCE:")
            print(f"Z-Score: {z_score:.2f}")
            if abs(z_score) > 1.96:
                print("[OK] Results are statistically significant (95% confidence)")
            else:
                print("[!] Need more data for statistical significance")
        
        print("\n" + "=" * 60)
    
    def cleanup(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main function to run watchlist tracker."""
    
    logger.info("Starting Watchlist Tracker...")
    
    try:
        tracker = WatchlistTracker()
        
        # Save today's predictions
        tracker.save_daily_predictions()
        
        # Evaluate predictions from 1, 3, and 5 days ago
        tracker.evaluate_predictions()
        
        # Calculate accuracy summary
        tracker.calculate_accuracy_summary()
        
        # Generate report
        tracker.generate_accuracy_report()
        
        tracker.cleanup()
        
        logger.info("Watchlist tracking completed")
        
    except Exception as e:
        logger.error(f"Error in watchlist tracking: {e}")
        raise


if __name__ == "__main__":
    main()
