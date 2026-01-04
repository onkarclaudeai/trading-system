#!/usr/bin/env python3
"""
Intraday Options Trading Watchlist Generator
Identifies stocks with institutional interest suitable for intraday options trading.
Focuses on: high liquidity, volume spikes, momentum, and volatility.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import pandas as pd
import logging
from typing import List, Dict
import numpy as np

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

# Stocks with liquid options (F&O segment) - Major stocks
# You can expand this list based on your broker's offerings
LIQUID_OPTIONS_STOCKS = [
    '500325',  # RELIANCE
    '500312',  # ONGC
    '532540',  # TCS
    '500180',  # HDFC BANK
    '500209',  # INFOSYS
    '532174',  # ICICI BANK
    '500875',  # ITC
    '532555',  # NTPC
    '500696',  # HINDUNILVR
    '500010',  # HDFC
    '507685',  # WIPRO
    '532281',  # HCLTECH
    '532215',  # AXISBANK
    '500870',  # ASIAN PAINTS
    '532454',  # BHARTIARTL
    '500112',  # SBIN
    '532977',  # BAJAJ-AUTO
    '500182',  # HERO MOTOCORP
    '532187',  # INDUSINDBK
    '500820',  # ASIAN PAINTS
]


def get_stock_data_for_options(conn, lookback_days: int = 15) -> pd.DataFrame:
    """
    Fetch recent stock data optimized for intraday options trading analysis.
    """
    query = """
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
        net_turnover as turnover
    FROM bhav_copy
    WHERE trading_date >= CURRENT_DATE - INTERVAL '%s days'
        AND close_price > 0
        AND no_of_shrs > 0
        AND close_price >= 100  -- Options usually available for stocks > ₹100
    ORDER BY sc_code, trading_date
    """
    
    logger.info(f"Fetching data for last {lookback_days} days for options analysis...")
    df = pd.read_sql_query(query, conn, params=(lookback_days,))
    logger.info(f"Loaded {len(df)} records for {df['sc_code'].nunique()} unique stocks")
    
    return df


def calculate_intraday_metrics(df: pd.DataFrame, min_days_required: int = 2) -> pd.DataFrame:
    """
    Calculate metrics specifically for intraday options trading.
    
    Args:
        df: DataFrame with stock data
        min_days_required: Minimum days of data required (default 2 - need at least today and yesterday)
    """
    logger.info("Calculating intraday-focused metrics...")
    
    stock_metrics = []
    stocks_with_insufficient_data = 0
    
    for sc_code, group in df.groupby('sc_code'):
        # Need at least 2 days (today and historical data for comparison)
        if len(group) < min_days_required:
            stocks_with_insufficient_data += 1
            continue
        
        group = group.sort_values('trading_date')
        latest = group.iloc[-1]
        historical = group.iloc[:-1]
        
        if len(historical) == 0:
            continue
        
        # Basic metrics
        avg_volume = historical['volume'].mean()
        avg_turnover = historical['turnover'].mean()
        avg_trades = historical['trades'].mean()
        
        volume_ratio = latest['volume'] / avg_volume if avg_volume > 0 else 0
        turnover_ratio = latest['turnover'] / avg_turnover if avg_turnover > 0 else 0
        trades_ratio = latest['trades'] / avg_trades if avg_trades > 0 else 0
        
        # Price metrics
        price_change_pct = ((latest['close_price'] - latest['prev_close']) / 
                           latest['prev_close'] * 100) if latest['prev_close'] > 0 else 0
        
        # Intraday range (volatility indicator)
        day_range = latest['high_price'] - latest['low_price']
        intraday_volatility_pct = (day_range / latest['open_price'] * 100) if latest['open_price'] > 0 else 0
        
        # Price position in range (0-100, where 100 = closed at high)
        if day_range > 0:
            price_strength = ((latest['close_price'] - latest['low_price']) / day_range) * 100
        else:
            price_strength = 50
        
        # Average True Range (ATR) - simplified version
        if len(historical) >= 3:
            ranges = []
            for idx, row in historical.iterrows():
                day_range = row['high_price'] - row['low_price']
                ranges.append(day_range)
            atr = np.mean(ranges[-5:]) if len(ranges) >= 5 else np.mean(ranges)
            atr_pct = (atr / latest['close_price'] * 100) if latest['close_price'] > 0 else 0
        else:
            atr_pct = intraday_volatility_pct
        
        # Momentum (3-day price change)
        if len(historical) >= 3:
            three_days_ago_price = historical.iloc[-3]['close_price']
            momentum_3d = ((latest['close_price'] - three_days_ago_price) / 
                          three_days_ago_price * 100) if three_days_ago_price > 0 else 0
        else:
            momentum_3d = price_change_pct
        
        # Consistency check (volume trend)
        if len(historical) >= 3:
            recent_volume_avg = historical.iloc[-3:]['volume'].mean()
            older_volume_avg = historical.iloc[:-3]['volume'].mean() if len(historical) > 3 else recent_volume_avg
            volume_trend = (recent_volume_avg / older_volume_avg) if older_volume_avg > 0 else 1
        else:
            volume_trend = volume_ratio
        
        # Liquidity score (turnover in crores)
        turnover_cr = latest['turnover'] / 10000000  # Convert to crores
        
        # Check if it's in liquid options list
        has_liquid_options = sc_code in LIQUID_OPTIONS_STOCKS
        
        stock_metrics.append({
            'sc_code': sc_code,
            'sc_name': latest['sc_name'],
            'trading_date': latest['trading_date'],
            'close_price': latest['close_price'],
            'price_change_pct': price_change_pct,
            'momentum_3d': momentum_3d,
            'volume': latest['volume'],
            'avg_volume': avg_volume,
            'volume_ratio': volume_ratio,
            'volume_trend': volume_trend,
            'turnover': latest['turnover'],
            'turnover_cr': turnover_cr,
            'turnover_ratio': turnover_ratio,
            'trades': latest['trades'],
            'trades_ratio': trades_ratio,
            'intraday_volatility_pct': intraday_volatility_pct,
            'atr_pct': atr_pct,
            'price_strength': price_strength,
            'day_high': latest['high_price'],
            'day_low': latest['low_price'],
            'has_liquid_options': has_liquid_options,
            'days_analyzed': len(group)
        })
    
    metrics_df = pd.DataFrame(stock_metrics)
    logger.info(f"Calculated metrics for {len(metrics_df)} stocks")
    if stocks_with_insufficient_data > 0:
        logger.warning(f"Skipped {stocks_with_insufficient_data} stocks with less than {min_days_required} days of data")
    
    return metrics_df


def calculate_options_trading_score(row) -> float:
    """
    Score specifically for intraday options trading potential.
    Focus: Liquidity, Volatility, Momentum, Volume surge
    """
    score = 0
    
    # 1. Volume surge with consistency (0-25 points)
    if row['volume_ratio'] >= 3 and row['volume_trend'] >= 1.2:
        score += 25
    elif row['volume_ratio'] >= 2.5:
        score += 20
    elif row['volume_ratio'] >= 2:
        score += 15
    elif row['volume_ratio'] >= 1.5:
        score += 10
    
    # 2. Strong momentum (0-20 points)
    if abs(row['momentum_3d']) > 5 and row['volume_ratio'] > 1.5:
        score += 20
    elif abs(row['momentum_3d']) > 3:
        score += 15
    elif abs(row['momentum_3d']) > 2:
        score += 10
    
    # 3. Intraday volatility (good for options) (0-15 points)
    if row['atr_pct'] >= 3:
        score += 15
    elif row['atr_pct'] >= 2:
        score += 12
    elif row['atr_pct'] >= 1.5:
        score += 8
    elif row['atr_pct'] >= 1:
        score += 5
    
    # 4. Liquidity (essential for options) (0-20 points)
    if row['turnover_cr'] >= 100:
        score += 20
    elif row['turnover_cr'] >= 50:
        score += 15
    elif row['turnover_cr'] >= 25:
        score += 10
    elif row['turnover_cr'] >= 10:
        score += 5
    
    # 5. Trade activity increase (0-10 points)
    if row['trades_ratio'] >= 2:
        score += 10
    elif row['trades_ratio'] >= 1.5:
        score += 7
    elif row['trades_ratio'] >= 1.2:
        score += 4
    
    # 6. Price strength (directional bias) (0-10 points)
    if row['price_strength'] >= 80:  # Strong bullish close
        score += 10
    elif row['price_strength'] >= 70:
        score += 7
    elif row['price_strength'] <= 20:  # Strong bearish close (can buy puts)
        score += 8
    elif row['price_strength'] <= 30:
        score += 5
    
    # Bonus: Known liquid options (0-10 points)
    if row['has_liquid_options']:
        score += 10
    
    return score


def suggest_option_strategy(row) -> Dict:
    """
    Suggest option strategy based on stock metrics.
    """
    strategy = {
        'direction': '',
        'strategy_type': '',
        'reasoning': '',
        'strike_suggestion': '',
        'risk_level': ''
    }
    
    # Determine direction
    bullish_signals = 0
    bearish_signals = 0
    
    if row['momentum_3d'] > 2:
        bullish_signals += 1
    elif row['momentum_3d'] < -2:
        bearish_signals += 1
    
    if row['price_strength'] >= 70:
        bullish_signals += 1
    elif row['price_strength'] <= 30:
        bearish_signals += 1
    
    if row['price_change_pct'] > 1:
        bullish_signals += 1
    elif row['price_change_pct'] < -1:
        bearish_signals += 1
    
    # Determine strategy
    if bullish_signals >= 2:
        strategy['direction'] = 'BULLISH'
        if row['atr_pct'] >= 2:
            strategy['strategy_type'] = 'BUY CALL (ATM/Slightly OTM)'
            strategy['reasoning'] = 'Strong upward momentum with high volatility'
            strategy['risk_level'] = 'MEDIUM-HIGH'
        else:
            strategy['strategy_type'] = 'BUY CALL (ATM)'
            strategy['reasoning'] = 'Consistent upward momentum'
            strategy['risk_level'] = 'MEDIUM'
        
        # Strike suggestion
        atm_strike = round(row['close_price'] / 50) * 50  # Round to nearest 50
        strategy['strike_suggestion'] = f"ATM: ₹{atm_strike}, OTM: ₹{atm_strike + 50}"
        
    elif bearish_signals >= 2:
        strategy['direction'] = 'BEARISH'
        if row['atr_pct'] >= 2:
            strategy['strategy_type'] = 'BUY PUT (ATM/Slightly OTM)'
            strategy['reasoning'] = 'Strong downward momentum with high volatility'
            strategy['risk_level'] = 'MEDIUM-HIGH'
        else:
            strategy['strategy_type'] = 'BUY PUT (ATM)'
            strategy['reasoning'] = 'Consistent downward momentum'
            strategy['risk_level'] = 'MEDIUM'
        
        atm_strike = round(row['close_price'] / 50) * 50
        strategy['strike_suggestion'] = f"ATM: ₹{atm_strike}, OTM: ₹{atm_strike - 50}"
        
    else:
        strategy['direction'] = 'NEUTRAL/WAIT'
        strategy['strategy_type'] = 'STRADDLE/STRANGLE or WAIT'
        strategy['reasoning'] = 'High volume but unclear direction'
        strategy['risk_level'] = 'HIGH'
        
        atm_strike = round(row['close_price'] / 50) * 50
        strategy['strike_suggestion'] = f"Straddle at ATM: ₹{atm_strike}"
    
    return strategy


def generate_options_watchlist(metrics_df: pd.DataFrame,
                               conn=None,
                               trading_date=None,
                               min_price: float = 100,
                               max_price: float = 10000,
                               min_volume_ratio: float = 1.3,
                               max_volume_ratio: float = 50,  # NEW: Filter extreme outliers
                               min_turnover_cr: float = 5,
                               min_score: float = 40,
                               top_n: int = 30,
                               use_market_context: bool = True,
                               min_days_for_analysis: int = 2) -> pd.DataFrame:  # NEW parameter
    """
    Generate watchlist specifically for intraday options trading.
    Now includes market context filtering for better accuracy.
    
    Args:
        min_days_for_analysis: Minimum days of data required (default 2, can be lowered to 1 for limited data)
    """
    logger.info("Generating options trading watchlist...")
    
    # Check if metrics_df is empty
    if len(metrics_df) == 0:
        logger.warning("No stocks available for analysis (empty metrics DataFrame)")
        return pd.DataFrame()
    
    # Calculate trading score
    metrics_df['options_score'] = metrics_df.apply(calculate_options_trading_score, axis=1)
    
    # Add data quality indicator
    metrics_df['data_quality'] = metrics_df['days_analyzed'].apply(
        lambda x: 'HIGH' if x >= 10 else 'MEDIUM' if x >= 5 else 'LOW'
    )
    
    # Filter for options trading suitability (more flexible with days requirement)
    watchlist = metrics_df[
        (metrics_df['close_price'] >= min_price) &
        (metrics_df['close_price'] <= max_price) &
        (metrics_df['volume_ratio'] >= min_volume_ratio) &
        (metrics_df['volume_ratio'] <= max_volume_ratio) &  # NEW: Cap outliers
        (metrics_df['turnover_cr'] >= min_turnover_cr) &
        (metrics_df['options_score'] >= min_score) &
        (metrics_df['days_analyzed'] >= min_days_for_analysis)  # Flexible threshold
    ].copy()
    
    # Log data quality warning if using limited data
    low_quality_count = len(watchlist[watchlist['data_quality'] == 'LOW'])
    if low_quality_count > 0:
        logger.warning(f"⚠️ {low_quality_count} stocks have LOW data quality (<5 days of history)")
        logger.warning("Results may be less reliable. Consider waiting for more data.")
    
    # Get market context if enabled
    market_context = None
    if use_market_context and conn is not None:
        try:
            from analysis.market_context_analyzer import analyze_market_context, should_take_trade
            
            if trading_date is None:
                trading_date = datetime.now().date()
            
            market_context = analyze_market_context(conn, trading_date)
            logger.info(f"Market Regime: {market_context['regime']}")
            
            # Log market breadth
            if market_context.get('breadth'):
                breadth = market_context['breadth']
                logger.info(f"Market Breadth: {breadth['advance_ratio']*100:.1f}% advances")
        
        except Exception as e:
            logger.warning(f"Could not load market context: {e}")
            use_market_context = False
    
    # Add strategy suggestions
    strategies = watchlist.apply(suggest_option_strategy, axis=1)
    watchlist['direction'] = strategies.apply(lambda x: x['direction'])
    watchlist['strategy_type'] = strategies.apply(lambda x: x['strategy_type'])
    watchlist['reasoning'] = strategies.apply(lambda x: x['reasoning'])
    watchlist['strike_suggestion'] = strategies.apply(lambda x: x['strike_suggestion'])
    watchlist['risk_level'] = strategies.apply(lambda x: x['risk_level'])
    
    # NEW: Apply market context filter
    if use_market_context and market_context is not None:
        watchlist['market_confidence'] = 0.5
        watchlist['market_filter_reason'] = ''
        watchlist['should_trade'] = True
        
        filtered_rows = []
        for idx, row in watchlist.iterrows():
            should_trade, reason, confidence = should_take_trade(
                row['direction'], 
                market_context
            )
            
            row['market_confidence'] = confidence
            row['market_filter_reason'] = reason
            row['should_trade'] = should_trade
            
            if should_trade:
                filtered_rows.append(row)
        
        if filtered_rows:
            watchlist = pd.DataFrame(filtered_rows)
            logger.info(f"Market filter: Kept {len(watchlist)} trades (filtered based on market context)")
        else:
            logger.warning("Market filter removed all trades - market conditions unfavorable")
            watchlist = pd.DataFrame()  # Empty
    
    # Sort by score
    if len(watchlist) > 0:
        watchlist = watchlist.sort_values('options_score', ascending=False)
        watchlist = watchlist.head(top_n)
    
    logger.info(f"Generated watchlist with {len(watchlist)} stocks")
    
    return watchlist


def generate_options_report(watchlist: pd.DataFrame) -> str:
    """
    Generate detailed report for options trading.
    """
    report_lines = []
    report_lines.append("=" * 120)
    report_lines.append("INTRADAY OPTIONS TRADING WATCHLIST")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append("Strategy: Identify stocks with institutional interest for intraday options trading")
    report_lines.append("=" * 120)
    report_lines.append("")
    
    if len(watchlist) == 0:
        report_lines.append("⚠️  No stocks found matching the criteria.")
        return "\n".join(report_lines)
    
    # Summary stats
    bullish_count = len(watchlist[watchlist['direction'] == 'BULLISH'])
    bearish_count = len(watchlist[watchlist['direction'] == 'BEARISH'])
    neutral_count = len(watchlist[watchlist['direction'] == 'NEUTRAL/WAIT'])
    
    report_lines.append(f"📊 SUMMARY:")
    report_lines.append(f"  Total Opportunities: {len(watchlist)}")
    report_lines.append(f"  🟢 Bullish Setups: {bullish_count}")
    report_lines.append(f"  🔴 Bearish Setups: {bearish_count}")
    report_lines.append(f"  ⚪ Neutral/Wait: {neutral_count}")
    report_lines.append("")
    report_lines.append("-" * 120)
    
    for idx, row in watchlist.iterrows():
        direction_emoji = "🟢" if row['direction'] == 'BULLISH' else "🔴" if row['direction'] == 'BEARISH' else "⚪"
        
        # Data quality indicator
        quality_indicator = ""
        if 'data_quality' in row:
            if row['data_quality'] == 'LOW':
                quality_indicator = " ⚠️ [LIMITED DATA]"
            elif row['data_quality'] == 'MEDIUM':
                quality_indicator = " [MODERATE DATA]"
        
        report_lines.append(f"\n{direction_emoji} {row['sc_name']} ({row['sc_code']}) - Score: {row['options_score']:.0f}/100{quality_indicator}")
        report_lines.append(f"  💰 Price: ₹{row['close_price']:.2f} | Change: {row['price_change_pct']:+.2f}% | 3D Momentum: {row['momentum_3d']:+.2f}%")
        report_lines.append(f"  📊 Volume: {row['volume']:,.0f} (Ratio: {row['volume_ratio']:.2f}x) | Turnover: ₹{row['turnover_cr']:.1f} Cr")
        report_lines.append(f"  📈 Volatility: {row['atr_pct']:.2f}% | Price Strength: {row['price_strength']:.1f}%")
        report_lines.append(f"  🎯 Direction: {row['direction']}")
        report_lines.append(f"  📋 Strategy: {row['strategy_type']}")
        report_lines.append(f"  💡 Strikes: {row['strike_suggestion']}")
        report_lines.append(f"  ⚠️  Risk: {row['risk_level']}")
        report_lines.append(f"  📝 Reasoning: {row['reasoning']}")
        
        # Show days analyzed for transparency
        report_lines.append(f"  📅 Data Points: {row['days_analyzed']} days")
        
        if row['has_liquid_options']:
            report_lines.append(f"  ✅ Known liquid options available")
        
        report_lines.append("-" * 120)
    
    report_lines.append("\n⚠️  DISCLAIMER:")
    report_lines.append("This is for educational/informational purposes only. Not financial advice.")
    report_lines.append("Always do your own research and manage risk appropriately.")
    report_lines.append("Options trading carries significant risk of loss.")
    
    return "\n".join(report_lines)


def main(lookback_days: int = 15,
         min_price: float = 100,
         max_price: float = 10000,
         min_volume_ratio: float = 1.3,
         max_volume_ratio: float = 50,  # NEW
         min_turnover_cr: float = 5,
         min_score: float = 40,
         top_n: int = 30,
         save_csv: bool = True,
         save_report: bool = True,
         use_market_context: bool = True,
         add_nse_symbols: bool = True,
         min_days_required: int = 2,  # NEW: Can be reduced to 1 for very limited data
         strict_mode: bool = False):  # NEW: If True, requires 5+ days like before
    """
    Main function for options trading watchlist.
    Now includes flexible data requirements and market context filtering.
    
    Args:
        min_days_required: Minimum days of data required (default 2, can be 1 for limited data)
        strict_mode: If True, requires at least 5 days of data for analysis (more reliable)
    """
    logger.info("Starting intraday options watchlist analysis...")
    
    # Override min_days if strict mode is enabled
    if strict_mode:
        min_days_required = max(5, min_days_required)
        logger.info("Strict mode enabled: Requiring at least 5 days of data")
    else:
        logger.info(f"Flexible mode: Using stocks with at least {min_days_required} day(s) of data")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Get data
        df = get_stock_data_for_options(conn, lookback_days)
        
        if len(df) == 0:
            logger.error("No data found. Run the Bhav copy downloader first.")
            return None
        
        # Calculate metrics with flexible days requirement
        metrics_df = calculate_intraday_metrics(df, min_days_required=min_days_required)
        
        # Check if metrics_df is empty before proceeding
        if len(metrics_df) == 0:
            logger.error("No stocks met the criteria for analysis. Possible reasons:")
            logger.error(f"1. No stocks have at least {min_days_required} day(s) of data")
            logger.error("2. No stocks meet the minimum requirements")
            logger.error(f"3. Check database for stocks with close_price >= 100 in last {lookback_days} days")
            logger.info("TIP: Try setting min_days_required=1 to use whatever data is available")
            return pd.DataFrame()
        
        # Log data availability
        data_summary = metrics_df['days_analyzed'].value_counts().sort_index()
        logger.info("Data availability summary:")
        for days, count in data_summary.items():
            logger.info(f"  {count} stocks with {days} days of data")
        
        # Generate watchlist with market context
        watchlist = generate_options_watchlist(
            metrics_df,
            conn=conn,
            trading_date=datetime.now().date(),
            min_price=min_price,
            max_price=max_price,
            min_volume_ratio=min_volume_ratio,
            max_volume_ratio=max_volume_ratio,
            min_turnover_cr=min_turnover_cr,
            min_score=min_score,
            top_n=top_n,
            use_market_context=use_market_context,
            min_days_for_analysis=min_days_required  # Pass the flexible requirement
        )
        
        # Add NSE symbols for Sharekhan
        if add_nse_symbols and len(watchlist) > 0:
            try:
                from analysis.stock_symbol_mapper import SymbolMapper
                logger.info("Adding NSE symbols for Sharekhan compatibility...")
                mapper = SymbolMapper()
                watchlist = mapper.batch_map(watchlist)
            except ImportError:
                logger.warning("Symbol mapper not found, skipping NSE symbol mapping")
            except Exception as e:
                logger.warning(f"Could not add NSE symbols: {e}")
        
        # Generate report with market context info
        report = generate_options_report(watchlist)
        
        # Add market context summary to report if available
        if use_market_context:
            try:
                from analysis.market_context_analyzer import analyze_market_context, generate_market_report
                context = analyze_market_context(conn, datetime.now().date())
                market_report = generate_market_report(context)
                print("\n" + market_report + "\n")
            except:
                pass
        
        print("\n" + report)
        
        # Save outputs
        if save_csv and len(watchlist) > 0:
            csv_file = f"options_watchlist_{datetime.now().strftime('%Y%m%d')}.csv"
            output_cols = ['sc_code', 'sc_name', 'close_price', 'price_change_pct', 
                          'momentum_3d', 'volume_ratio', 'turnover_cr', 'atr_pct',
                          'options_score', 'direction', 'strategy_type', 'strike_suggestion',
                          'days_analyzed', 'data_quality']  # Added data columns
            
            # Add NSE symbol if available
            if 'nse_symbol' in watchlist.columns:
                output_cols.insert(2, 'nse_symbol')
            
            # Add market context columns if available
            if 'market_confidence' in watchlist.columns:
                output_cols.extend(['market_confidence', 'market_filter_reason'])
            
            # Only include columns that exist
            output_cols = [col for col in output_cols if col in watchlist.columns]
            
            watchlist[output_cols].to_csv(csv_file, index=False)
            logger.info(f"Watchlist saved to {csv_file}")
        
        if save_report and len(watchlist) > 0:
            report_file = f"options_report_{datetime.now().strftime('%Y%m%d')}.txt"
            full_report = report
            
            if use_market_context:
                try:
                    from analysis.market_context_analyzer import analyze_market_context, generate_market_report
                    context = analyze_market_context(conn, datetime.now().date())
                    market_report = generate_market_report(context)
                    full_report = market_report + "\n\n" + report
                except:
                    pass
            
            with open(report_file, 'w') as f:
                f.write(full_report)
            logger.info(f"Report saved to {report_file}")
        
        logger.info("Analysis completed successfully!")
        return watchlist
        
    except Exception as e:
        logger.error(f"❌ Error generating watchlist: {e}")
        logger.error(f"{e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        conn.close()


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


def run_with_limited_data(**kwargs):
    """
    Convenience function to run analysis with whatever data is available.
    This is useful when you have just started collecting data.
    
    Example:
        watchlist = run_with_limited_data()  # Will work with as little as 1 day of data
    """
    logger.info("Running in LIMITED DATA mode - using whatever data is available")
    kwargs['min_days_required'] = kwargs.get('min_days_required', 1)
    kwargs['strict_mode'] = False
    kwargs['min_score'] = kwargs.get('min_score', 30)  # Lower score threshold for limited data
    
    return main(**kwargs)


def run_strict_analysis(**kwargs):
    """
    Run analysis with strict data quality requirements (5+ days minimum).
    This provides more reliable results but requires more historical data.
    
    Example:
        watchlist = run_strict_analysis()  # Requires at least 5 days of data
    """
    logger.info("Running in STRICT mode - requiring high data quality")
    kwargs['strict_mode'] = True
    kwargs['min_days_required'] = max(5, kwargs.get('min_days_required', 5))
    
    return main(**kwargs)


if __name__ == "__main__":
    import sys
    
    # Check for command-line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == 'limited':
            # Run with minimal data requirements
            print("Running with LIMITED DATA mode...")
            run_with_limited_data()
        elif mode == 'strict':
            # Run with strict data requirements
            print("Running with STRICT mode...")
            run_strict_analysis()
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python intraday_options_watchlist.py [limited|strict]")
            print("  limited - Use whatever data is available (even 1 day)")
            print("  strict  - Require at least 5 days of data")
    else:
        # Default mode - flexible but prefer more data
        print("Running in DEFAULT mode (minimum 2 days of data)")
        print("TIP: Use 'python intraday_options_watchlist.py limited' for minimal data")
        print("     Use 'python intraday_options_watchlist.py strict' for high quality")
        main(min_days_required=2)
