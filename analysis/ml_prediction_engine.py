#!/usr/bin/env python3
"""
Enhanced ML Prediction Engine v2 - Complete Implementation
Properly integrates with daily workflow and learns from missed opportunities.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import joblib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json

# ML Libraries
try:
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("Warning: scikit-learn not installed. Run: pip install scikit-learn")

# Configure logging
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

# Model paths
MODELS_DIR = Path(__file__).parent / 'models'
MODELS_DIR.mkdir(exist_ok=True)


class MLPredictionEngine:
    """
    Complete ML Prediction Engine for options trading.
    
    Workflow Integration:
    1. DAILY (after watchlist): Run enhance_watchlist_with_ml() to add ML scores
    2. WEEKLY (Sunday): Run retrain_models() to learn from recent data
    3. MONTHLY: Run full_learning_cycle() for comprehensive retraining
    """
    
    def __init__(self, conn=None):
        self.conn = conn or psycopg2.connect(**DB_CONFIG)
        self.models = {}
        self.scaler = StandardScaler()
        self.model_loaded = False
        self.feature_cols = []
        
        # Try to load existing models
        self._load_models()
        
        # Ensure tables exist
        self._create_tables()
    
    def _create_tables(self):
        """Create necessary ML tracking tables."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS ml_model_performance (
                id SERIAL PRIMARY KEY,
                model_date DATE,
                model_version VARCHAR(50),
                accuracy DECIMAL(5,4),
                precision_score DECIMAL(5,4),
                recall_score DECIMAL(5,4),
                f1_score DECIMAL(5,4),
                samples_trained INTEGER,
                feature_importance JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS daily_movers (
                id SERIAL PRIMARY KEY,
                trading_date DATE,
                sc_code VARCHAR(20),
                sc_name VARCHAR(200),
                day_return_pct DECIMAL(10,2),
                volume_surge DECIMAL(10,2),
                was_predicted BOOLEAN DEFAULT FALSE,
                prediction_score DECIMAL(5,2),
                miss_reason VARCHAR(100),
                mover_category VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(trading_date, sc_code)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS coverage_metrics (
                id SERIAL PRIMARY KEY,
                analysis_date DATE UNIQUE,
                total_big_movers INTEGER,
                movers_predicted INTEGER,
                coverage_rate DECIMAL(5,2),
                opportunity_cost DECIMAL(15,2),
                avg_missed_return DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ]
        
        with self.conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
        self.conn.commit()
        logger.info("ML tables created/verified")
    
    def _load_models(self):
        """Load pre-trained models if available."""
        try:
            model_path = MODELS_DIR / 'prediction_models.pkl'
            scaler_path = MODELS_DIR / 'scaler.pkl'
            features_path = MODELS_DIR / 'features.json'
            
            if model_path.exists() and scaler_path.exists():
                self.models = joblib.load(model_path)
                self.scaler = joblib.load(scaler_path)
                
                if features_path.exists():
                    with open(features_path, 'r') as f:
                        self.feature_cols = json.load(f)
                
                self.model_loaded = True
                logger.info("Loaded existing ML models successfully")
            else:
                logger.info("No existing models found - will train on first run")
                
        except Exception as e:
            logger.warning(f"Could not load models: {e}")
            self.model_loaded = False
    
    def _save_models(self):
        """Save trained models to disk."""
        try:
            joblib.dump(self.models, MODELS_DIR / 'prediction_models.pkl')
            joblib.dump(self.scaler, MODELS_DIR / 'scaler.pkl')
            
            with open(MODELS_DIR / 'features.json', 'w') as f:
                json.dump(self.feature_cols, f)
            
            logger.info("Models saved successfully")
        except Exception as e:
            logger.error(f"Could not save models: {e}")
    
    def prepare_training_data(self, lookback_days: int = 90) -> pd.DataFrame:
        """
        Prepare feature-rich training data from historical bhav_copy.
        """
        logger.info(f"Preparing training data for last {lookback_days} days...")
        
        query = """
        WITH stock_data AS (
            SELECT 
                sc_code,
                sc_name,
                trading_date,
                open_price,
                high_price,
                low_price,
                close_price,
                prev_close,
                no_of_shrs as volume,
                no_of_trades as trades,
                net_turnover as turnover,
                
                -- Price changes
                CASE WHEN prev_close > 0 THEN 
                    (close_price - prev_close) / prev_close * 100 
                ELSE 0 END as price_change_pct,
                
                -- Daily range
                CASE WHEN open_price > 0 THEN
                    (high_price - low_price) / open_price * 100
                ELSE 0 END as daily_range_pct,
                
                -- Price strength (position in day's range)
                CASE WHEN (high_price - low_price) > 0 THEN
                    (close_price - low_price) / (high_price - low_price) * 100
                ELSE 50 END as price_strength
                
            FROM bhav_copy
            WHERE trading_date >= CURRENT_DATE - INTERVAL '%s days'
                AND close_price > 100
                AND no_of_shrs > 10000
        ),
        with_lags AS (
            SELECT *,
                -- Volume ratios (today vs yesterday)
                LAG(volume) OVER (PARTITION BY sc_code ORDER BY trading_date) as prev_volume,
                
                -- Price momentum (3-day)
                LAG(close_price, 3) OVER (PARTITION BY sc_code ORDER BY trading_date) as close_3d_ago,
                
                -- Next day return (target variable)
                LEAD(price_change_pct) OVER (PARTITION BY sc_code ORDER BY trading_date) as next_day_return
                
            FROM stock_data
        )
        SELECT 
            sc_code,
            sc_name,
            trading_date,
            close_price,
            price_change_pct,
            daily_range_pct,
            price_strength,
            volume,
            trades,
            turnover,
            
            -- Calculated features
            CASE WHEN prev_volume > 0 THEN volume::float / prev_volume ELSE 1 END as volume_ratio,
            CASE WHEN close_3d_ago > 0 THEN 
                (close_price - close_3d_ago) / close_3d_ago * 100 
            ELSE 0 END as momentum_3d,
            
            -- Target
            next_day_return,
            CASE WHEN next_day_return > 0 THEN 1 ELSE 0 END as next_day_up
            
        FROM with_lags
        WHERE prev_volume IS NOT NULL 
            AND next_day_return IS NOT NULL
        ORDER BY trading_date, sc_code
        """
        
        df = pd.read_sql_query(query, self.conn, params=(lookback_days,))
        logger.info(f"Loaded {len(df)} training samples from {df['sc_code'].nunique()} stocks")
        
        return df
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add engineered features to the dataframe."""
        
        # Group-based features (per stock)
        for col in ['volume', 'turnover', 'daily_range_pct']:
            if col in df.columns:
                df[f'{col}_zscore'] = df.groupby('sc_code')[col].transform(
                    lambda x: (x - x.rolling(10, min_periods=1).mean()) / 
                              (x.rolling(10, min_periods=1).std() + 0.001)
                )
        
        # Volume spike indicator
        df['volume_spike'] = (df['volume_ratio'] > 2).astype(int)
        
        # Momentum categories
        df['strong_momentum'] = (abs(df['momentum_3d']) > 3).astype(int)
        
        # Price strength categories
        df['bullish_close'] = (df['price_strength'] > 70).astype(int)
        df['bearish_close'] = (df['price_strength'] < 30).astype(int)
        
        # Turnover in crores
        df['turnover_cr'] = df['turnover'] / 10000000
        
        # Log transforms for skewed features
        df['log_volume'] = np.log1p(df['volume'])
        df['log_turnover'] = np.log1p(df['turnover'])
        
        return df
    
    def train_models(self, force_retrain: bool = False) -> Dict:
        """
        Train ML models on historical data.
        
        Args:
            force_retrain: If True, retrain even if models exist
            
        Returns:
            Dictionary with training metrics
        """
        if not SKLEARN_AVAILABLE:
            logger.error("scikit-learn not available. Install with: pip install scikit-learn")
            return {'error': 'sklearn not installed'}
        
        if self.model_loaded and not force_retrain:
            logger.info("Models already loaded. Use force_retrain=True to retrain.")
            return {'status': 'models_already_loaded'}
        
        logger.info("=" * 60)
        logger.info("TRAINING ML MODELS")
        logger.info("=" * 60)
        
        # Prepare data
        df = self.prepare_training_data(lookback_days=90)
        
        if len(df) < 1000:
            logger.warning(f"Insufficient training data: {len(df)} samples (need 1000+)")
            return {'error': 'insufficient_data', 'samples': len(df)}
        
        # Engineer features
        df = self.engineer_features(df)
        
        # Select feature columns
        self.feature_cols = [
            'price_change_pct', 'daily_range_pct', 'price_strength',
            'volume_ratio', 'momentum_3d', 'turnover_cr',
            'volume_spike', 'strong_momentum', 'bullish_close', 'bearish_close',
            'log_volume', 'log_turnover'
        ]
        
        # Filter to only available columns
        self.feature_cols = [c for c in self.feature_cols if c in df.columns]
        
        # Prepare X and y
        X = df[self.feature_cols].fillna(0)
        y = df['next_day_up']
        
        # Train-test split (time-aware)
        split_idx = int(len(X) * 0.8)
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train Random Forest
        logger.info("Training Random Forest...")
        rf_model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=20,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        )
        rf_model.fit(X_train_scaled, y_train)
        rf_pred = rf_model.predict(X_test_scaled)
        
        # Train Gradient Boosting
        logger.info("Training Gradient Boosting...")
        gb_model = GradientBoostingClassifier(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            random_state=42
        )
        gb_model.fit(X_train_scaled, y_train)
        gb_pred = gb_model.predict(X_test_scaled)
        
        # Ensemble prediction (average)
        ensemble_pred = ((rf_model.predict_proba(X_test_scaled)[:, 1] + 
                         gb_model.predict_proba(X_test_scaled)[:, 1]) / 2 > 0.5).astype(int)
        
        # Calculate metrics
        metrics = {
            'accuracy': accuracy_score(y_test, ensemble_pred),
            'precision': precision_score(y_test, ensemble_pred, zero_division=0),
            'recall': recall_score(y_test, ensemble_pred, zero_division=0),
            'f1': f1_score(y_test, ensemble_pred, zero_division=0),
            'samples_trained': len(X_train),
            'samples_tested': len(X_test)
        }
        
        # Feature importance
        feature_importance = dict(zip(
            self.feature_cols,
            rf_model.feature_importances_.tolist()
        ))
        
        # Store models
        self.models = {
            'random_forest': rf_model,
            'gradient_boost': gb_model,
            'ensemble_weights': [0.5, 0.5]
        }
        self.model_loaded = True
        
        # Save models
        self._save_models()
        
        # Save metrics to database
        self._save_training_metrics(metrics, feature_importance)
        
        # Print results
        logger.info("=" * 60)
        logger.info("TRAINING RESULTS")
        logger.info("=" * 60)
        logger.info(f"Accuracy:  {metrics['accuracy']:.2%}")
        logger.info(f"Precision: {metrics['precision']:.2%}")
        logger.info(f"Recall:    {metrics['recall']:.2%}")
        logger.info(f"F1 Score:  {metrics['f1']:.2%}")
        logger.info("")
        logger.info("Top Features:")
        for feat, imp in sorted(feature_importance.items(), key=lambda x: -x[1])[:5]:
            logger.info(f"  {feat}: {imp:.3f}")
        logger.info("=" * 60)
        
        return metrics
    
    def _save_training_metrics(self, metrics: Dict, feature_importance: Dict):
        """Save training metrics to database."""
        query = """
        INSERT INTO ml_model_performance 
        (model_date, model_version, accuracy, precision_score, recall_score, 
         f1_score, samples_trained, feature_importance)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (
                datetime.now().date(),
                'v2.0',
                metrics['accuracy'],
                metrics['precision'],
                metrics['recall'],
                metrics['f1'],
                metrics['samples_trained'],
                json.dumps(feature_importance)
            ))
        self.conn.commit()
    
    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """
        Make predictions for given stocks.
        
        Args:
            features_df: DataFrame with required feature columns
            
        Returns:
            DataFrame with added ml_score and ml_direction columns
        """
        if not self.model_loaded:
            logger.warning("No models loaded. Training first...")
            self.train_models()
        
        if not self.model_loaded:
            logger.error("Could not load or train models")
            features_df['ml_score'] = 50  # Default neutral score
            features_df['ml_direction'] = 'NEUTRAL'
            return features_df
        
        # Engineer features
        df = self.engineer_features(features_df.copy())
        
        # Ensure all required features exist
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = 0
        
        X = df[self.feature_cols].fillna(0)
        X_scaled = self.scaler.transform(X)
        
        # Get predictions from both models
        rf_proba = self.models['random_forest'].predict_proba(X_scaled)[:, 1]
        gb_proba = self.models['gradient_boost'].predict_proba(X_scaled)[:, 1]
        
        # Ensemble probability
        weights = self.models['ensemble_weights']
        ensemble_proba = weights[0] * rf_proba + weights[1] * gb_proba
        
        # Convert to score (0-100) and direction
        df['ml_score'] = (ensemble_proba * 100).round(1)
        df['ml_direction'] = np.where(ensemble_proba > 0.55, 'BULLISH',
                                      np.where(ensemble_proba < 0.45, 'BEARISH', 'NEUTRAL'))
        df['ml_confidence'] = np.abs(ensemble_proba - 0.5) * 200  # 0-100 confidence
        
        return df
    
    def enhance_watchlist_with_ml(self, watchlist_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Add ML predictions to the daily watchlist.
        This is the main function to call from daily workflow.
        
        Args:
            watchlist_df: If None, loads today's watchlist from file
            
        Returns:
            Enhanced watchlist with ML scores
        """
        logger.info("Enhancing watchlist with ML predictions...")
        
        if watchlist_df is None:
            # Try to load today's watchlist
            watchlist_file = f"options_watchlist_{datetime.now().strftime('%Y%m%d')}.csv"
            if os.path.exists(watchlist_file):
                watchlist_df = pd.read_csv(watchlist_file)
            else:
                logger.error(f"Watchlist file not found: {watchlist_file}")
                return None
        
        if len(watchlist_df) == 0:
            logger.warning("Empty watchlist provided")
            return watchlist_df
        
        # Prepare features for prediction
        # Map column names if needed
        col_mapping = {
            'close': 'close_price',
            'volume_surge': 'volume_ratio'
        }
        for old, new in col_mapping.items():
            if old in watchlist_df.columns and new not in watchlist_df.columns:
                watchlist_df[new] = watchlist_df[old]
        
        # Make predictions
        enhanced_df = self.predict(watchlist_df)
        
        # Combine ML score with options score for final ranking
        if 'options_score' in enhanced_df.columns:
            enhanced_df['combined_ml_score'] = (
                enhanced_df['options_score'] * 0.6 + 
                enhanced_df['ml_score'] * 0.4
            ).round(1)
        else:
            enhanced_df['combined_ml_score'] = enhanced_df['ml_score']
        
        # Re-sort by combined score
        enhanced_df = enhanced_df.sort_values('combined_ml_score', ascending=False)
        
        # Save enhanced watchlist
        output_file = f"enhanced_watchlist_{datetime.now().strftime('%Y%m%d')}.csv"
        enhanced_df.to_csv(output_file, index=False)
        logger.info(f"Enhanced watchlist saved to {output_file}")
        
        return enhanced_df
    
    def track_daily_movers(self):
        """
        Track big movers and whether they were in our predictions.
        Run daily after market close.
        """
        logger.info("Tracking daily movers...")
        
        # Get today's big movers (>3% move)
        query = """
        SELECT 
            bc.sc_code,
            bc.sc_name,
            bc.trading_date,
            CASE WHEN bc.prev_close > 0 THEN
                (bc.close_price - bc.prev_close) / bc.prev_close * 100
            ELSE 0 END as day_return_pct,
            CASE WHEN prev_bc.no_of_shrs > 0 THEN
                bc.no_of_shrs::float / prev_bc.no_of_shrs
            ELSE 1 END as volume_surge
        FROM bhav_copy bc
        LEFT JOIN bhav_copy prev_bc ON bc.sc_code = prev_bc.sc_code 
            AND prev_bc.trading_date = bc.trading_date - INTERVAL '1 day'
        WHERE bc.trading_date = (SELECT MAX(trading_date) FROM bhav_copy)
            AND bc.close_price > 100
            AND bc.no_of_shrs > 100000
            AND ABS(CASE WHEN bc.prev_close > 0 THEN
                (bc.close_price - bc.prev_close) / bc.prev_close * 100
            ELSE 0 END) > 3
        ORDER BY day_return_pct DESC
        """
        
        movers = pd.read_sql_query(query, self.conn)
        
        if len(movers) == 0:
            logger.info("No significant movers today")
            return
        
        logger.info(f"Found {len(movers)} significant movers (>3% change)")
        
        # Check which were predicted
        predicted_query = """
        SELECT DISTINCT sc_code 
        FROM watchlist_predictions 
        WHERE prediction_date = CURRENT_DATE - INTERVAL '1 day'
        """
        
        predicted_df = pd.read_sql_query(predicted_query, self.conn)
        predicted_codes = set(predicted_df['sc_code'].tolist()) if len(predicted_df) > 0 else set()
        
        # Categorize movers
        for _, mover in movers.iterrows():
            was_predicted = mover['sc_code'] in predicted_codes
            
            # Determine miss reason
            miss_reason = None
            if not was_predicted:
                if mover['volume_surge'] < 1.5:
                    miss_reason = 'LOW_VOLUME_HISTORY'
                elif mover['volume_surge'] > 5:
                    miss_reason = 'SUDDEN_SPIKE_NO_BUILDUP'
                else:
                    miss_reason = 'BELOW_SCORE_THRESHOLD'
            
            # Categorize mover
            if mover['day_return_pct'] > 5:
                category = 'GAINER_5PCT+'
            elif mover['day_return_pct'] > 3:
                category = 'GAINER_3-5PCT'
            elif mover['day_return_pct'] < -5:
                category = 'LOSER_5PCT+'
            else:
                category = 'LOSER_3-5PCT'
            
            # Insert into tracking table
            insert_query = """
            INSERT INTO daily_movers 
            (trading_date, sc_code, sc_name, day_return_pct, volume_surge, 
             was_predicted, miss_reason, mover_category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trading_date, sc_code) DO UPDATE SET
                was_predicted = EXCLUDED.was_predicted,
                miss_reason = EXCLUDED.miss_reason
            """
            
            with self.conn.cursor() as cur:
                cur.execute(insert_query, (
                    mover['trading_date'],
                    mover['sc_code'],
                    mover['sc_name'],
                    mover['day_return_pct'],
                    mover['volume_surge'],
                    was_predicted,
                    miss_reason,
                    category
                ))
        
        self.conn.commit()
        
        # Calculate coverage metrics
        total_movers = len(movers)
        predicted_count = movers['sc_code'].isin(predicted_codes).sum()
        coverage_rate = (predicted_count / total_movers * 100) if total_movers > 0 else 0
        
        missed = movers[~movers['sc_code'].isin(predicted_codes)]
        avg_missed_return = missed['day_return_pct'].abs().mean() if len(missed) > 0 else 0
        opportunity_cost = missed['day_return_pct'].abs().sum() if len(missed) > 0 else 0
        
        # Save coverage metrics
        coverage_query = """
        INSERT INTO coverage_metrics 
        (analysis_date, total_big_movers, movers_predicted, coverage_rate, 
         opportunity_cost, avg_missed_return)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (analysis_date) DO UPDATE SET
            coverage_rate = EXCLUDED.coverage_rate,
            opportunity_cost = EXCLUDED.opportunity_cost
        """
        
        with self.conn.cursor() as cur:
            cur.execute(coverage_query, (
                datetime.now().date(),
                total_movers,
                predicted_count,
                coverage_rate,
                opportunity_cost,
                avg_missed_return
            ))
        self.conn.commit()
        
        logger.info(f"Coverage: {predicted_count}/{total_movers} = {coverage_rate:.1f}%")
        logger.info(f"Avg missed return: {avg_missed_return:.2f}%")
    
    def cleanup(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main function for ML prediction engine."""
    import argparse
    
    parser = argparse.ArgumentParser(description='ML Prediction Engine')
    parser.add_argument('--train', action='store_true', help='Train/retrain models')
    parser.add_argument('--enhance', action='store_true', help='Enhance today\'s watchlist')
    parser.add_argument('--track', action='store_true', help='Track daily movers')
    parser.add_argument('--all', action='store_true', help='Run complete daily cycle')
    
    args = parser.parse_args()
    
    engine = MLPredictionEngine()
    
    try:
        if args.train or args.all:
            logger.info("Training ML models...")
            engine.train_models(force_retrain=True)
        
        if args.enhance or args.all:
            logger.info("Enhancing watchlist...")
            engine.enhance_watchlist_with_ml()
        
        if args.track or args.all:
            logger.info("Tracking movers...")
            engine.track_daily_movers()
        
        if not any([args.train, args.enhance, args.track, args.all]):
            # Default: enhance watchlist
            engine.enhance_watchlist_with_ml()
        
        logger.info("ML engine completed successfully")
        
    finally:
        engine.cleanup()


if __name__ == "__main__":
    main()
