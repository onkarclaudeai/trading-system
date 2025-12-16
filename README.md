# Intraday Options Trading System - Fixed & Enhanced

## Files to Replace/Add

| New File | Replaces | Purpose |
|----------|----------|---------|
| `daily_flow_orchestrator_v2.py` | `daily_flow_orchestrator.py` | Main workflow orchestrator |
| `ml_prediction_engine_v2.py` | `ml_prediction_engine.py` | ML predictions (complete implementation) |
| `options_integration_v2.py` | NEW | Options trading setup generator |
| `nse_fo_downloader_v2.py` | `nse_fo_downloader.py` | Fixed F&O downloader |
| `paper_trading_tracker_patched.py` | `paper_trading_tracker.py` | Fixed column compatibility |
| `watchlist_tracker_patched.py` | `watchlist_tracker.py` | Fixed column compatibility |

**Keep your existing files unchanged:**
- `bse_bhav_downloader.py`
- `intraday_options_watchlist.py`
- `market_context_analyzer.py`
- `morning_premarket_alert.py`
- `gap_filler.py`
- `intraday_scanner.py`
- `stock_symbol_mapper.py`
- All `fix_*.py` files

---

## Quick Start

Run the complete daily workflow:
```bash
python daily_flow_orchestrator_v2.py
```

## Your Two Issues - SOLVED

### Issue 1: When to Run the ML Engine

The ML engine is now **automatically integrated** into the daily workflow. Here's the schedule:

| Task | When | Command |
|------|------|---------|
| **Daily Enhancement** | Every day after watchlist generation | Automatic in daily workflow |
| **Weekly Retraining** | Sundays (recommended) | `python ml_prediction_engine_v2.py --train` |
| **First-time Training** | First run | Automatic (if no models exist) |

The orchestrator handles this automatically:
```python
# In daily_flow_orchestrator_v2.py:
# 1. Generates watchlist
# 2. Enhances with ML scores automatically
# 3. Saves enhanced_watchlist_YYYYMMDD.csv
```

**Manual ML Commands:**
```bash
# Train/retrain models
python ml_prediction_engine_v2.py --train

# Enhance today's watchlist
python ml_prediction_engine_v2.py --enhance

# Track daily movers (for learning)
python ml_prediction_engine_v2.py --track

# Do everything
python ml_prediction_engine_v2.py --all
```

### Issue 2: Options Data & Automatic Trade Setups

**BEFORE:** You had to manually look up options for each stock in the watchlist.

**NOW:** The system automatically generates an **Options Trading Sheet** with:
- Specific strike prices (ATM, OTM)
- Estimated premiums
- Lot sizes
- Risk/reward calculations
- Entry/exit strategies

The daily workflow now outputs:
```
options_watchlist_YYYYMMDD.csv      # Base watchlist
enhanced_watchlist_YYYYMMDD.csv    # With ML scores
options_trading_sheet_YYYYMMDD.csv # Ready-to-trade setups!
options_trading_report_YYYYMMDD.txt # Formatted report
```

**Sample Options Trading Sheet:**
```
Symbol  | Direction | Spot    | Option | Strike | Type | Expiry     | Lot  | Premium | Max Risk | R:R
--------|-----------|---------|--------|--------|------|------------|------|---------|----------|----
RELIANCE| BULLISH   | 2450.00 | CE     | 2460   | ATM  | 2025-01-16 | 250  | 45.00   | 11250    | 1.5
RELIANCE| BULLISH   | 2450.00 | CE     | 2480   | OTM  | 2025-01-16 | 250  | 28.00   | 7000     | 2.0
HDFCBANK| BULLISH   | 1680.00 | CE     | 1680   | ATM  | 2025-01-16 | 550  | 22.00   | 12100    | 1.5
```

---

## Complete Workflow Schedule

### Daily (After Market Close - 4:00 PM)
```bash
# Complete workflow - does everything
python daily_flow_orchestrator_v2.py

# Quick run (skip ML and tracking)
python daily_flow_orchestrator_v2.py --quick

# Skip downloads (if re-running same day)
python daily_flow_orchestrator_v2.py --skip-downloads
```

### Weekly (Sunday)
```bash
# Retrain ML models with recent data
python ml_prediction_engine_v2.py --train
```

### Pre-Market (8:30 AM)
```bash
# Generate morning alert with gap risk analysis
python morning_premarket_alert.py
```

---

## File Structure After Running

```
your_project/
├── logs/
│   └── daily_workflow_YYYYMMDD.log
├── models/
│   ├── prediction_models.pkl
│   ├── scaler.pkl
│   └── features.json
├── options_watchlist_YYYYMMDD.csv       # Base watchlist
├── enhanced_watchlist_YYYYMMDD.csv      # With ML scores
├── options_trading_sheet_YYYYMMDD.csv   # Trade setups!
├── options_trading_report_YYYYMMDD.txt  # Formatted report
├── morning_alert_YYYYMMDD.txt           # Pre-market alert
└── daily_summary_YYYYMMDD.txt           # Workflow summary
```

---

## What Each New Module Does

### 1. `ml_prediction_engine_v2.py`
- **Purpose:** Adds ML-based prediction scores to watchlist
- **Features:**
  - Random Forest + Gradient Boosting ensemble
  - Learns from missed opportunities
  - Tracks prediction accuracy over time
  
### 2. `options_integration_v2.py`
- **Purpose:** Converts watchlist stocks to tradeable options setups
- **Features:**
  - Maps BSE codes to NSE F&O symbols
  - Calculates appropriate strikes (ATM, OTM)
  - Estimates premiums
  - Provides lot sizes and risk calculations
  - Generates ready-to-trade output

### 3. `daily_flow_orchestrator_v2.py`
- **Purpose:** Runs the complete daily workflow
- **Phases:**
  1. Data Collection (Bhav copy download)
  2. Analysis & Watchlist Generation
  3. ML Enhancement
  4. Options Setup Generation
  5. Alert Generation
  6. Accuracy Tracking
  7. Maintenance (gap checking)

### 4. `nse_fo_downloader_v2.py`
- **Purpose:** Downloads F&O data from NSE (fixed, no circular imports)

---

## Configuration

### Database
All modules use these environment variables (or defaults):
```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=postgres
export DB_USER=postgres
export DB_PASSWORD=postgres
```

### Cron Jobs (Recommended)
```bash
# Daily after market (4:30 PM IST)
30 16 * * 1-5 cd /path/to/project && python daily_flow_orchestrator_v2.py >> logs/cron.log 2>&1

# Morning alert (8:45 AM IST)
45 8 * * 1-5 cd /path/to/project && python morning_premarket_alert.py >> logs/cron.log 2>&1

# Weekly ML retrain (Sunday 10 AM)
0 10 * * 0 cd /path/to/project && python ml_prediction_engine_v2.py --train >> logs/cron.log 2>&1
```

---

## F&O Stocks Supported

The system automatically maps these BSE stocks to NSE F&O symbols:

| BSE Code | NSE Symbol | Lot Size | Strike Interval |
|----------|------------|----------|-----------------|
| 500325   | RELIANCE   | 250      | 20              |
| 532540   | TCS        | 175      | 50              |
| 500180   | HDFCBANK   | 550      | 20              |
| 500209   | INFY       | 400      | 25              |
| 532174   | ICICIBANK  | 700      | 10              |
| 500112   | SBIN       | 1500     | 10              |
| ... and 25+ more |

Non-F&O stocks are automatically skipped when generating options setups.

---

## Troubleshooting

### "No F&O stocks in watchlist"
- Your watchlist contains only non-F&O stocks
- Wait for F&O stocks to meet volume/momentum criteria

### "No models found"
- First run - models will be trained automatically
- Or run: `python ml_prediction_engine_v2.py --train`

### "Empty watchlist"
- No stocks met the criteria
- Check if bhav_copy data was downloaded
- Try: `python intraday_options_watchlist.py limited`

### Database connection error
- Check PostgreSQL is running
- Verify credentials in environment variables

---

## Migration from Old System

1. Copy new files to your project:
   - `ml_prediction_engine_v2.py`
   - `options_integration_v2.py`
   - `daily_flow_orchestrator_v2.py`
   - `nse_fo_downloader_v2.py`

2. Run once to create new tables:
   ```bash
   python daily_flow_orchestrator_v2.py
   ```

3. Update your cron jobs to use `daily_flow_orchestrator_v2.py`

Your existing data and tables are preserved - the new modules add to them.

---

## Compatibility Notes

The new modules are designed to work with your existing files. However, there were **two compatibility issues** in your original files:

### Issue: Column Name Mismatch

| Your watchlist outputs | Trackers expected |
|------------------------|-------------------|
| `direction` | `predicted_direction` |
| `options_score` | `combined_score` |

**Solution:** Use the patched tracker files:
- `paper_trading_tracker_patched.py` → replaces `paper_trading_tracker.py`
- `watchlist_tracker_patched.py` → replaces `watchlist_tracker.py`

These patched versions handle both column naming conventions.

### If you don't want to replace files

You can also fix the original files by changing:

**In `paper_trading_tracker.py` (line ~189):**
```python
# Change this:
if 'predicted_direction' in row:
    direction = row['predicted_direction']

# To this:
if 'direction' in row:
    direction = row['direction']
elif 'predicted_direction' in row:
    direction = row['predicted_direction']
```

**In `watchlist_tracker.py` (line ~189):**
```python
# Change this:
if 'predicted_direction' in row:
    predicted_direction = row['predicted_direction']

# To this:
if 'direction' in row:
    predicted_direction = row['direction']
elif 'predicted_direction' in row:
    predicted_direction = row['predicted_direction']
```

---

## Output Example

After running `python daily_flow_orchestrator_v2.py`:

```
======================================================================
STARTING ENHANCED DAILY WORKFLOW - 2025-01-15 16:30:00
======================================================================

PHASE 1: DATA COLLECTION
[OK] Bhav copy downloaded

PHASE 2: ANALYSIS & WATCHLIST GENERATION
[OK] Market context analyzed
[OK] Watchlist generated with 25 stocks

PHASE 3: ML PREDICTION ENHANCEMENT
[OK] ML enhancement complete - 25 stocks scored

PHASE 4: OPTIONS TRADING SETUPS
[OK] Generated 18 options trading setups

PHASE 5: ALERTS GENERATION
[OK] Morning alert generated

PHASE 6: ACCURACY TRACKING
[OK] Accuracy tracking completed
[OK] Paper trading updated (3 new trades)
[OK] Daily movers tracked

PHASE 7: MAINTENANCE
[OK] No data gaps found

======================================================================
[OK] DAILY WORKFLOW COMPLETED SUCCESSFULLY
Total time: 45.2 seconds
======================================================================
```

Check `options_trading_sheet_YYYYMMDD.csv` for your trade setups!
