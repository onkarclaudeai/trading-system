#!/usr/bin/env python3
"""
Enhanced Daily Trading Workflow Orchestrator v2
Properly integrates ML predictions and options trading setups.

Workflow:
1. Download BSE Bhav Copy data
2. Analyze market context
3. Generate base watchlist (volume surge + momentum)
4. Enhance with ML predictions
5. Generate options trading setups (specific strikes, premiums)
6. Generate morning alert
7. Track previous predictions accuracy
8. Paper trading updates
9. Gap checking
"""

import os
import sys
import psycopg2
from datetime import datetime, timedelta
import logging
import traceback
from pathlib import Path
import time

# Setup logging
LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

log_filename = LOG_DIR / f'daily_workflow_{datetime.now().strftime("%Y%m%d")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
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


class DailyWorkflowV2:
    """Enhanced orchestrator for daily trading workflow."""
    
    def __init__(self, skip_downloads: bool = False):
        """
        Initialize workflow.
        
        Args:
            skip_downloads: If True, skip data downloads (useful for re-runs)
        """
        self.workflow_date = datetime.now()
        self.conn = None
        self.skip_downloads = skip_downloads
        self.results = {}
        self.connect_db()
    
    def connect_db(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            logger.info("[OK] Database connected successfully")
        except Exception as e:
            logger.error(f"[FAIL] Database connection failed: {e}")
            raise
    
    def run_complete_workflow(self, 
                             run_ml: bool = True,
                             run_options: bool = True,
                             run_tracking: bool = True):
        """
        Run the complete daily workflow.
        
        Args:
            run_ml: Run ML prediction enhancement
            run_options: Generate options trading setups
            run_tracking: Run accuracy tracking and paper trading
        """
        
        logger.info("=" * 80)
        logger.info(f"STARTING ENHANCED DAILY WORKFLOW - {self.workflow_date.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        try:
            # ==========================================
            # PHASE 1: DATA COLLECTION
            # ==========================================
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 1: DATA COLLECTION")
            logger.info("=" * 80)
            
            if not self.skip_downloads:
                self.download_bhav_copy()
            else:
                logger.info("[SKIP] Bhav copy download skipped")
            
            # ==========================================
            # PHASE 2: ANALYSIS & WATCHLIST
            # ==========================================
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 2: ANALYSIS & WATCHLIST GENERATION")
            logger.info("=" * 80)
            
            self.analyze_market_context()
            watchlist = self.generate_watchlist()
            
            # ==========================================
            # PHASE 3: ML ENHANCEMENT
            # ==========================================
            if run_ml:
                logger.info("\n" + "=" * 80)
                logger.info("PHASE 3: ML PREDICTION ENHANCEMENT")
                logger.info("=" * 80)
                
                self.run_ml_predictions()
            
            # ==========================================
            # PHASE 4: OPTIONS INTEGRATION
            # ==========================================
            if run_options:
                logger.info("\n" + "=" * 80)
                logger.info("PHASE 4: OPTIONS TRADING SETUPS")
                logger.info("=" * 80)
                
                self.generate_options_setups()
            
            # ==========================================
            # PHASE 5: ALERTS
            # ==========================================
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 5: ALERTS GENERATION")
            logger.info("=" * 80)
            
            self.generate_morning_alert()
            
            # ==========================================
            # PHASE 6: TRACKING & ANALYSIS
            # ==========================================
            if run_tracking:
                logger.info("\n" + "=" * 80)
                logger.info("PHASE 6: ACCURACY TRACKING")
                logger.info("=" * 80)
                
                self.track_watchlist_accuracy()
                self.run_paper_trading()
                self.track_daily_movers()
            
            # ==========================================
            # PHASE 7: MAINTENANCE
            # ==========================================
            logger.info("\n" + "=" * 80)
            logger.info("PHASE 7: MAINTENANCE")
            logger.info("=" * 80)
            
            self.check_data_gaps()
            
            # ==========================================
            # FINAL REPORT
            # ==========================================
            self.generate_daily_report()
            
            elapsed_time = time.time() - start_time
            
            logger.info("\n" + "=" * 80)
            logger.info("[OK] DAILY WORKFLOW COMPLETED SUCCESSFULLY")
            logger.info(f"Total time: {elapsed_time:.1f} seconds")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error(f"[FAIL] Workflow failed: {e}")
            logger.error(traceback.format_exc())
            raise
        finally:
            if self.conn:
                self.conn.close()
    
    def download_bhav_copy(self):
        """Download BSE Bhav Copy - CRITICAL."""
        logger.info("Step 1.1: Downloading BSE Bhav Copy...")
        
        try:
            import bse_bhav_downloader
            
            # Download today's data only (more efficient)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=1)
            
            bse_bhav_downloader.process_date_range(start_date, end_date)
            
            logger.info("[OK] Bhav copy downloaded")
            self.results['bhav_copy'] = 'success'
            
        except Exception as e:
            logger.error(f"[FAIL] Bhav copy download failed: {e}")
            self.results['bhav_copy'] = f'failed: {e}'
            # This is critical - we might want to stop here
            raise
    
    def analyze_market_context(self):
        """Analyze market context."""
        logger.info("Step 2.1: Analyzing market context...")
        
        try:
            import market_context_analyzer
            
            if hasattr(market_context_analyzer, 'main'):
                market_context_analyzer.main()
            
            logger.info("[OK] Market context analyzed")
            self.results['market_context'] = 'success'
            
        except ImportError:
            logger.info("[SKIP] market_context_analyzer not available")
            self.results['market_context'] = 'skipped'
        except Exception as e:
            logger.warning(f"[WARN] Market context analysis failed: {e}")
            self.results['market_context'] = f'failed: {e}'
    
    def generate_watchlist(self):
        """Generate daily watchlist - CRITICAL."""
        logger.info("Step 2.2: Generating watchlist...")
        
        try:
            import intraday_options_watchlist
            
            # Run with default settings
            watchlist = intraday_options_watchlist.main(
                min_days_required=2,
                save_csv=True,
                save_report=True,
                add_nse_symbols=True,
                use_market_context=True
            )
            
            if watchlist is not None and len(watchlist) > 0:
                logger.info(f"[OK] Watchlist generated with {len(watchlist)} stocks")
                self.results['watchlist'] = f'success: {len(watchlist)} stocks'
            else:
                logger.warning("[WARN] Empty watchlist generated")
                self.results['watchlist'] = 'empty'
            
            return watchlist
            
        except Exception as e:
            logger.error(f"[FAIL] Watchlist generation failed: {e}")
            self.results['watchlist'] = f'failed: {e}'
            raise
    
    def run_ml_predictions(self):
        """Run ML predictions to enhance watchlist."""
        logger.info("Step 3.1: Running ML predictions...")
        
        try:
            # Try the new ML engine first
            try:
                from ml_prediction_engine_v2 import MLPredictionEngine
                
                engine = MLPredictionEngine(self.conn)
                
                # Train if needed (first time or weekly refresh)
                if not engine.model_loaded:
                    logger.info("Training ML models (first time)...")
                    engine.train_models()
                
                # Enhance watchlist
                enhanced = engine.enhance_watchlist_with_ml()
                
                if enhanced is not None and len(enhanced) > 0:
                    logger.info(f"[OK] ML enhancement complete - {len(enhanced)} stocks scored")
                    self.results['ml_predictions'] = f'success: {len(enhanced)} scored'
                else:
                    logger.info("[OK] ML models ready (no enhancement needed)")
                    self.results['ml_predictions'] = 'success'
                    
            except ImportError:
                # Fall back to old ML engine
                logger.info("Using legacy ML engine...")
                import ml_prediction_engine
                
                if hasattr(ml_prediction_engine, 'adaptive_learning_cycle'):
                    ml_prediction_engine.adaptive_learning_cycle()
                    logger.info("[OK] ML learning cycle completed (legacy)")
                    self.results['ml_predictions'] = 'success (legacy)'
                else:
                    logger.warning("[WARN] ML engine not properly configured")
                    self.results['ml_predictions'] = 'partial'
                    
        except Exception as e:
            logger.warning(f"[WARN] ML predictions failed: {e}")
            self.results['ml_predictions'] = f'failed: {e}'
            # Non-critical, continue
    
    def generate_options_setups(self):
        """Generate options trading setups with specific strikes."""
        logger.info("Step 4.1: Generating options trading setups...")
        
        try:
            # Try the new options integration module
            try:
                from options_integration_v2 import OptionsIntegration
                
                integration = OptionsIntegration(self.conn)
                setups = integration.process_watchlist()
                
                if setups is not None and len(setups) > 0:
                    logger.info(f"[OK] Generated {len(setups)} options trading setups")
                    self.results['options_setups'] = f'success: {len(setups)} setups'
                else:
                    logger.info("[OK] No F&O stocks in watchlist")
                    self.results['options_setups'] = 'no F&O stocks'
                
                integration.cleanup()
                
            except ImportError:
                # Fall back to options chain collector
                logger.info("Options integration module not found, using basic collector...")
                import options_chain_collector
                
                if hasattr(options_chain_collector, 'collect_all_fno_options'):
                    options_chain_collector.collect_all_fno_options()
                    logger.info("[OK] Options data collected (basic)")
                    self.results['options_setups'] = 'success (basic)'
                else:
                    logger.warning("[WARN] Options collector not available")
                    self.results['options_setups'] = 'skipped'
                    
        except Exception as e:
            logger.warning(f"[WARN] Options setup generation failed: {e}")
            self.results['options_setups'] = f'failed: {e}'
            # Non-critical, continue
    
    def generate_morning_alert(self):
        """Generate morning pre-market alert."""
        logger.info("Step 5.1: Generating morning alert...")
        
        try:
            # Try different module names
            for module_name in ['morning_premarket_alert', 'morning_pre_market_alert']:
                try:
                    module = __import__(module_name)
                    if hasattr(module, 'main'):
                        module.main()
                        logger.info("[OK] Morning alert generated")
                        self.results['morning_alert'] = 'success'
                        return
                except ImportError:
                    continue
            
            logger.info("[SKIP] Morning alert module not available")
            self.results['morning_alert'] = 'skipped'
            
        except Exception as e:
            logger.warning(f"[WARN] Morning alert failed: {e}")
            self.results['morning_alert'] = f'failed: {e}'
    
    def track_watchlist_accuracy(self):
        """Track watchlist prediction accuracy."""
        logger.info("Step 6.1: Tracking prediction accuracy...")
        
        try:
            import watchlist_tracker
            
            tracker = watchlist_tracker.WatchlistTracker()
            
            # Save today's predictions
            tracker.save_daily_predictions()
            
            # Evaluate past predictions
            tracker.evaluate_predictions()
            
            # Calculate summary
            tracker.calculate_accuracy_summary()
            
            # Generate report
            tracker.generate_accuracy_report()
            
            tracker.cleanup()
            
            logger.info("[OK] Accuracy tracking completed")
            self.results['accuracy_tracking'] = 'success'
            
        except ImportError:
            logger.info("[SKIP] Watchlist tracker not available")
            self.results['accuracy_tracking'] = 'skipped'
        except Exception as e:
            logger.warning(f"[WARN] Accuracy tracking failed: {e}")
            self.results['accuracy_tracking'] = f'failed: {e}'
    
    def run_paper_trading(self):
        """Run paper trading system."""
        logger.info("Step 6.2: Running paper trading...")
        
        try:
            import paper_trading_tracker
            
            tracker = paper_trading_tracker.PaperTradingTracker()
            
            # Enter new trades from watchlist
            trades_entered = tracker.enter_trades_from_watchlist()
            
            # Update open positions
            tracker.update_open_trades()
            
            # Calculate performance
            tracker.calculate_daily_performance()
            
            # Generate report
            tracker.generate_report()
            
            tracker.cleanup()
            
            logger.info(f"[OK] Paper trading updated ({trades_entered} new trades)")
            self.results['paper_trading'] = f'success: {trades_entered} trades'
            
        except ImportError:
            logger.info("[SKIP] Paper trading not available")
            self.results['paper_trading'] = 'skipped'
        except Exception as e:
            logger.warning(f"[WARN] Paper trading failed: {e}")
            self.results['paper_trading'] = f'failed: {e}'
    
    def track_daily_movers(self):
        """Track big movers and missed opportunities."""
        logger.info("Step 6.3: Tracking daily movers...")
        
        try:
            try:
                from ml_prediction_engine_v2 import MLPredictionEngine
                
                engine = MLPredictionEngine(self.conn)
                engine.track_daily_movers()
                
                logger.info("[OK] Daily movers tracked")
                self.results['movers_tracking'] = 'success'
                
            except ImportError:
                logger.info("[SKIP] Movers tracking requires ml_prediction_engine_v2")
                self.results['movers_tracking'] = 'skipped'
                
        except Exception as e:
            logger.warning(f"[WARN] Movers tracking failed: {e}")
            self.results['movers_tracking'] = f'failed: {e}'
    
    def check_data_gaps(self):
        """Check for data gaps."""
        logger.info("Step 7.1: Checking data gaps...")
        
        try:
            import gap_filler
            
            filler = gap_filler.DataGapFiller()
            
            # Check last 7 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            bhav_gaps = filler.find_bhav_copy_gaps(start_date, end_date)
            watchlist_gaps = filler.find_watchlist_gaps(start_date, end_date)
            
            if bhav_gaps or watchlist_gaps:
                logger.warning(f"[WARN] Found gaps - Bhav: {len(bhav_gaps)}, Watchlist: {len(watchlist_gaps)}")
                
                # Auto-fill only critical gaps (last 2 days)
                critical_date = datetime.now() - timedelta(days=2)
                critical_gaps = [g for g in bhav_gaps if g >= critical_date]
                
                if critical_gaps:
                    logger.info(f"Auto-filling {len(critical_gaps)} critical gaps...")
                    filler.fill_bhav_copy_gaps(critical_gaps, max_days=2)
                
                self.results['gap_check'] = f'gaps found: {len(bhav_gaps)} bhav, {len(watchlist_gaps)} watchlist'
            else:
                logger.info("[OK] No data gaps found")
                self.results['gap_check'] = 'no gaps'
            
            filler.cleanup()
            
        except ImportError:
            logger.info("[SKIP] Gap filler not available")
            self.results['gap_check'] = 'skipped'
        except Exception as e:
            logger.warning(f"[WARN] Gap checking failed: {e}")
            self.results['gap_check'] = f'failed: {e}'
    
    def generate_daily_report(self):
        """Generate consolidated daily report."""
        logger.info("Step 7.2: Generating daily report...")
        
        try:
            report = []
            report.append("=" * 70)
            report.append("DAILY TRADING WORKFLOW SUMMARY")
            report.append(f"Date: {self.workflow_date.strftime('%Y-%m-%d %H:%M')}")
            report.append("=" * 70)
            
            # Results summary
            report.append("\n[WORKFLOW RESULTS]")
            for step, result in self.results.items():
                status = "OK" if 'success' in str(result).lower() else "WARN"
                report.append(f"  [{status}] {step}: {result}")
            
            # Files generated
            report.append("\n[FILES GENERATED]")
            date_str = self.workflow_date.strftime('%Y%m%d')
            
            files_to_check = [
                f"options_watchlist_{date_str}.csv",
                f"enhanced_watchlist_{date_str}.csv",
                f"options_trading_sheet_{date_str}.csv",
                f"options_trading_report_{date_str}.txt",
                f"options_report_{date_str}.txt",
                f"morning_alert_{date_str}.txt",
            ]
            
            for file in files_to_check:
                if os.path.exists(file):
                    report.append(f"  [OK] {file}")
            
            # Quick stats
            report.append("\n[QUICK STATS]")
            
            try:
                import pandas as pd
                watchlist_file = f"options_watchlist_{date_str}.csv"
                if os.path.exists(watchlist_file):
                    df = pd.read_csv(watchlist_file)
                    report.append(f"  Total watchlist stocks: {len(df)}")
                    
                    if 'options_score' in df.columns:
                        report.append(f"  Avg confidence score: {df['options_score'].mean():.1f}")
                
                trading_file = f"options_trading_sheet_{date_str}.csv"
                if os.path.exists(trading_file):
                    df = pd.read_csv(trading_file)
                    report.append(f"  Options setups generated: {len(df)}")
                    
            except Exception as e:
                report.append(f"  Could not load stats: {e}")
            
            report.append("\n" + "=" * 70)
            
            summary_text = "\n".join(report)
            
            # Save summary
            summary_file = f"daily_summary_{date_str}.txt"
            with open(summary_file, 'w') as f:
                f.write(summary_text)
            
            print(summary_text)
            logger.info(f"[OK] Daily summary saved to {summary_file}")
            
        except Exception as e:
            logger.warning(f"[WARN] Report generation failed: {e}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily Trading Workflow Orchestrator v2')
    parser.add_argument('--skip-downloads', action='store_true', 
                       help='Skip data downloads (for re-runs)')
    parser.add_argument('--no-ml', action='store_true',
                       help='Skip ML predictions')
    parser.add_argument('--no-options', action='store_true',
                       help='Skip options setup generation')
    parser.add_argument('--no-tracking', action='store_true',
                       help='Skip accuracy tracking')
    parser.add_argument('--quick', action='store_true',
                       help='Quick run: skip ML, tracking')
    
    args = parser.parse_args()
    
    logger.info("Starting Daily Trading Workflow v2...")
    
    try:
        workflow = DailyWorkflowV2(skip_downloads=args.skip_downloads)
        
        run_ml = not (args.no_ml or args.quick)
        run_options = not args.no_options
        run_tracking = not (args.no_tracking or args.quick)
        
        workflow.run_complete_workflow(
            run_ml=run_ml,
            run_options=run_options,
            run_tracking=run_tracking
        )
        
        logger.info("[OK] Daily workflow completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"[FAIL] Fatal error in workflow: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
