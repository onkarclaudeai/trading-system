#!/usr/bin/env python3
"""
Market Context Analyzer
Analyzes overall market sentiment, breadth, and regime to improve prediction accuracy.
Helps filter predictions based on favorable market conditions.
"""

import os
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, Tuple

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


def get_market_breadth(conn, trading_date) -> Dict:
    """
    Calculate market breadth indicators for a given date.
    Returns advance/decline ratio, up/down volume, etc.
    """
    query = """
    SELECT 
        COUNT(*) as total_stocks,
        SUM(CASE WHEN close_price > prev_close THEN 1 ELSE 0 END) as advances,
        SUM(CASE WHEN close_price < prev_close THEN 1 ELSE 0 END) as declines,
        SUM(CASE WHEN close_price = prev_close THEN 1 ELSE 0 END) as unchanged,
        SUM(CASE WHEN close_price > prev_close THEN no_of_shrs ELSE 0 END) as up_volume,
        SUM(CASE WHEN close_price < prev_close THEN no_of_shrs ELSE 0 END) as down_volume,
        AVG((close_price - prev_close) / NULLIF(prev_close, 0) * 100) as avg_change_pct,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (close_price - prev_close) / NULLIF(prev_close, 0) * 100) as median_change_pct
    FROM bhav_copy
    WHERE trading_date = %s
        AND prev_close > 0
        AND no_of_shrs > 0
        AND close_price > 0
    """
    
    df = pd.read_sql_query(query, conn, params=(trading_date,))
    
    if len(df) == 0 or df['total_stocks'].iloc[0] == 0:
        logger.warning(f"No market data for {trading_date}")
        return None
    
    row = df.iloc[0]
    
    # Calculate ratios
    advance_ratio = row['advances'] / row['total_stocks']
    decline_ratio = row['declines'] / row['total_stocks']
    
    # Volume ratio (bullish if >1)
    total_volume = row['up_volume'] + row['down_volume']
    up_volume_ratio = row['up_volume'] / total_volume if total_volume > 0 else 0.5
    
    breadth = {
        'date': trading_date,
        'total_stocks': int(row['total_stocks']),
        'advances': int(row['advances']),
        'declines': int(row['declines']),
        'unchanged': int(row['unchanged']),
        'advance_ratio': advance_ratio,
        'decline_ratio': decline_ratio,
        'up_volume_ratio': up_volume_ratio,
        'avg_change_pct': float(row['avg_change_pct']),
        'median_change_pct': float(row['median_change_pct'])
    }
    
    return breadth


def calculate_market_regime(conn, trading_date, lookback_days=10) -> Dict:
    """
    Determine market regime over recent period.
    Returns: STRONG_BULLISH, BULLISH, NEUTRAL, BEARISH, STRONG_BEARISH
    """
    start_date = trading_date - timedelta(days=lookback_days)
    
    query = """
    SELECT 
        trading_date,
        COUNT(*) as total_stocks,
        SUM(CASE WHEN close_price > prev_close THEN 1 ELSE 0 END) as advances,
        AVG((close_price - prev_close) / NULLIF(prev_close, 0) * 100) as avg_change_pct
    FROM bhav_copy
    WHERE trading_date BETWEEN %s AND %s
        AND prev_close > 0
        AND no_of_shrs > 0
    GROUP BY trading_date
    ORDER BY trading_date
    """
    
    df = pd.read_sql_query(query, conn, params=(start_date, trading_date))
    
    if len(df) == 0:
        logger.warning(f"No market data for regime calculation")
        return None
    
    # Calculate metrics
    df['advance_ratio'] = df['advances'] / df['total_stocks']
    
    avg_advance_ratio = df['advance_ratio'].mean()
    avg_change_pct = df['avg_change_pct'].mean()
    
    # Count bullish/bearish days
    bullish_days = (df['advance_ratio'] > 0.5).sum()
    bearish_days = (df['advance_ratio'] < 0.5).sum()
    
    # Determine regime
    if avg_advance_ratio >= 0.60 and avg_change_pct > 0.5:
        regime = 'STRONG_BULLISH'
        score = 5
    elif avg_advance_ratio >= 0.55:
        regime = 'BULLISH'
        score = 4
    elif avg_advance_ratio >= 0.45:
        regime = 'NEUTRAL'
        score = 3
    elif avg_advance_ratio >= 0.40:
        regime = 'BEARISH'
        score = 2
    else:
        regime = 'STRONG_BEARISH'
        score = 1
    
    regime_data = {
        'regime': regime,
        'regime_score': score,
        'avg_advance_ratio': avg_advance_ratio,
        'avg_change_pct': avg_change_pct,
        'bullish_days': int(bullish_days),
        'bearish_days': int(bearish_days),
        'total_days': len(df),
        'consistency': max(bullish_days, bearish_days) / len(df)  # How consistent the trend is
    }
    
    return regime_data


def get_sector_performance(conn, trading_date, lookback_days=5) -> pd.DataFrame:
    """
    Identify which sectors are performing well.
    Note: BSE data doesn't have explicit sector tags, so we approximate using stock groups.
    """
    start_date = trading_date - timedelta(days=lookback_days)
    
    query = """
    SELECT 
        sc_group,
        COUNT(DISTINCT sc_code) as stock_count,
        AVG((close_price - prev_close) / NULLIF(prev_close, 0) * 100) as avg_return,
        SUM(CASE WHEN close_price > prev_close THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as advance_pct
    FROM bhav_copy
    WHERE trading_date BETWEEN %s AND %s
        AND prev_close > 0
        AND no_of_shrs > 0
        AND sc_group IS NOT NULL
        AND sc_group != ''
    GROUP BY sc_group
    HAVING COUNT(DISTINCT sc_code) >= 5  -- At least 5 stocks in group
    ORDER BY avg_return DESC
    """
    
    df = pd.read_sql_query(query, conn, params=(start_date, trading_date))
    
    return df


def should_take_trade(stock_direction: str, market_context: Dict, 
                     stock_metrics: Dict = None) -> Tuple[bool, str, float]:
    """
    Decide whether to take a trade based on market context.
    
    Args:
        stock_direction: 'BULLISH' or 'BEARISH'
        market_context: Dict with regime and breadth info
        stock_metrics: Optional dict with stock-specific metrics
    
    Returns:
        (should_trade: bool, reason: str, confidence: float)
    """
    
    if market_context is None:
        return True, "No market context available, proceeding", 0.5
    
    regime = market_context.get('regime', 'UNKNOWN')
    regime_score = market_context.get('regime_score', 3)
    breadth = market_context.get('breadth') or {}  # Handle None breadth
    
    confidence = 0.5  # Base confidence
    reasons = []
    
    # Rules for BULLISH trades
    if stock_direction == 'BULLISH':
        # Strong market support needed for bullish
        if regime in ['STRONG_BULLISH', 'BULLISH']:
            confidence += 0.3
            reasons.append(f"✅ Market is {regime}")
        elif regime == 'NEUTRAL':
            confidence += 0.1
            reasons.append(f"⚠️ Market is {regime} (marginal)")
        else:
            confidence -= 0.3
            reasons.append(f"❌ Market is {regime} (against trade)")
        
        # Check breadth (only if available)
        advance_ratio = breadth.get('advance_ratio', 0.5)
        if advance_ratio > 0.60:
            confidence += 0.2
            reasons.append(f"✅ Strong breadth ({advance_ratio*100:.1f}% advances)")
        elif advance_ratio < 0.45:
            confidence -= 0.2
            reasons.append(f"❌ Weak breadth ({advance_ratio*100:.1f}% advances)")
        
        # Check volume (only if available)
        up_volume_ratio = breadth.get('up_volume_ratio', 0.5)
        if up_volume_ratio > 0.60:
            confidence += 0.1
            reasons.append(f"✅ Strong up volume")
        
        # Minimum threshold
        should_trade = confidence >= 0.60
        
        if not should_trade:
            reasons.insert(0, "🚫 SKIP BULLISH - Market conditions unfavorable")
        else:
            reasons.insert(0, "✅ TAKE BULLISH - Market conditions favorable")
    
    # Rules for BEARISH trades
    elif stock_direction == 'BEARISH':
        # Bearish works in any market except strong bullish
        if regime in ['STRONG_BEARISH', 'BEARISH']:
            confidence += 0.3
            reasons.append(f"✅ Market is {regime}")
        elif regime == 'NEUTRAL':
            confidence += 0.2
            reasons.append(f"✅ Market is {regime} (neutral OK for bearish)")
        elif regime == 'BULLISH':
            confidence += 0.0
            reasons.append(f"⚠️ Market is {regime} (proceed with caution)")
        else:  # STRONG_BULLISH
            confidence -= 0.2
            reasons.append(f"⚠️ Market is {regime} (difficult environment)")
        
        # Check breadth (only if available)
        decline_ratio = breadth.get('decline_ratio', 0.5)
        if decline_ratio > 0.55:
            confidence += 0.2
            reasons.append(f"✅ Strong decline breadth ({decline_ratio*100:.1f}% declines)")
        
        # Check volume (only if available)
        up_volume_ratio = breadth.get('up_volume_ratio', 0.5)
        if up_volume_ratio < 0.40:
            confidence += 0.1
            reasons.append(f"✅ Strong down volume")
        
        # Bearish is generally easier - lower threshold
        should_trade = confidence >= 0.50
        
        if not should_trade:
            reasons.insert(0, "🚫 SKIP BEARISH - Very strong bull market")
        else:
            reasons.insert(0, "✅ TAKE BEARISH - Conditions acceptable")
    
    else:  # NEUTRAL/WAIT
        should_trade = True
        confidence = 0.5
        reasons = ["⚪ Neutral strategy - market context less relevant"]
    
    reason_str = " | ".join(reasons)
    
    return should_trade, reason_str, confidence


def analyze_market_context(conn, trading_date) -> Dict:
    """
    Comprehensive market context analysis for a trading date.
    """
    logger.info(f"Analyzing market context for {trading_date}")
    
    # Get breadth
    breadth = get_market_breadth(conn, trading_date)
    
    # Get regime
    regime = calculate_market_regime(conn, trading_date, lookback_days=10)
    
    # Get sector performance
    sectors = get_sector_performance(conn, trading_date, lookback_days=5)
    
    context = {
        'date': trading_date,
        'breadth': breadth,
        'regime': regime['regime'] if regime else 'UNKNOWN',
        'regime_score': regime['regime_score'] if regime else 3,
        'regime_details': regime,
        'top_sectors': sectors.head(5).to_dict('records') if len(sectors) > 0 else [],
        'bottom_sectors': sectors.tail(5).to_dict('records') if len(sectors) > 0 else []
    }
    
    return context


def generate_market_report(context: Dict) -> str:
    """Generate human-readable market context report."""
    
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append(f"MARKET CONTEXT ANALYSIS - {context['date']}")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # Regime
    regime_emoji = {
        'STRONG_BULLISH': '🟢🟢',
        'BULLISH': '🟢',
        'NEUTRAL': '⚪',
        'BEARISH': '🔴',
        'STRONG_BEARISH': '🔴🔴'
    }
    
    regime = context['regime']
    regime_details = context.get('regime_details', {})
    
    report_lines.append(f"📊 MARKET REGIME: {regime_emoji.get(regime, '?')} {regime}")
    report_lines.append(f"   Score: {context['regime_score']}/5")
    if regime_details:
        report_lines.append(f"   10-Day Advance Ratio: {regime_details.get('avg_advance_ratio', 0)*100:.1f}%")
        report_lines.append(f"   Avg Daily Change: {regime_details.get('avg_change_pct', 0):+.2f}%")
        report_lines.append(f"   Bullish Days: {regime_details.get('bullish_days', 0)}/{regime_details.get('total_days', 0)}")
        report_lines.append(f"   Trend Consistency: {regime_details.get('consistency', 0)*100:.1f}%")
    report_lines.append("")
    
    # Breadth
    breadth = context.get('breadth', {})
    if breadth:
        report_lines.append(f"📈 TODAY'S MARKET BREADTH:")
        report_lines.append(f"   Advances: {breadth['advances']} ({breadth['advance_ratio']*100:.1f}%)")
        report_lines.append(f"   Declines: {breadth['declines']} ({breadth['decline_ratio']*100:.1f}%)")
        report_lines.append(f"   Up Volume Ratio: {breadth['up_volume_ratio']*100:.1f}%")
        report_lines.append(f"   Average Change: {breadth['avg_change_pct']:+.2f}%")
        report_lines.append(f"   Median Change: {breadth['median_change_pct']:+.2f}%")
        report_lines.append("")
    
    # Trading recommendations
    report_lines.append("🎯 TRADING RECOMMENDATIONS:")
    
    if regime in ['STRONG_BULLISH', 'BULLISH']:
        report_lines.append("   ✅ BULLISH trades: FAVORABLE - Market supports upside")
        report_lines.append("   ⚠️ BEARISH trades: CAUTION - Against trend")
    elif regime == 'NEUTRAL':
        report_lines.append("   ⚠️ BULLISH trades: MARGINAL - Require strong stock signals")
        report_lines.append("   ✅ BEARISH trades: ACCEPTABLE - Neutral market OK for shorts")
    elif regime in ['BEARISH', 'STRONG_BEARISH']:
        report_lines.append("   🚫 BULLISH trades: AVOID - Fighting the market")
        report_lines.append("   ✅ BEARISH trades: FAVORABLE - Market supports downside")
    
    report_lines.append("")
    
    # Sector leaders
    if context.get('top_sectors'):
        report_lines.append("🔥 TOP PERFORMING SECTORS (5-day):")
        for sector in context['top_sectors'][:5]:
            report_lines.append(f"   {sector['sc_group']}: {sector['avg_return']:+.2f}% "
                              f"({sector['advance_pct']*100:.0f}% advancing)")
    
    if context.get('bottom_sectors'):
        report_lines.append("")
        report_lines.append("❄️ BOTTOM PERFORMING SECTORS (5-day):")
        for sector in context['bottom_sectors'][:5]:
            report_lines.append(f"   {sector['sc_group']}: {sector['avg_return']:+.2f}% "
                              f"({sector['advance_pct']*100:.0f}% advancing)")
    
    report_lines.append("")
    report_lines.append("=" * 80)
    
    return "\n".join(report_lines)


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


# Convenience functions for Jupyter
def get_today_context():
    """Get market context for today."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        today = datetime.now().date()
        context = analyze_market_context(conn, today)
        report = generate_market_report(context)
        print(report)
        return context
    finally:
        conn.close()


def check_trade(direction, trading_date=None):
    """Quick check if a trade direction makes sense today."""
    if trading_date is None:
        trading_date = datetime.now().date()
    
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        context = analyze_market_context(conn, trading_date)
        should_trade, reason, confidence = should_take_trade(direction, context)
        
        print(f"\n{'='*60}")
        print(f"Trade Check: {direction} on {trading_date}")
        print(f"{'='*60}")
        print(f"Decision: {'✅ TAKE TRADE' if should_trade else '🚫 SKIP TRADE'}")
        print(f"Confidence: {confidence*100:.1f}%")
        print(f"Reasoning: {reason}")
        print(f"{'='*60}\n")
        
        return should_trade, confidence
    finally:
        conn.close()


if __name__ == "__main__":
    # Example usage
    get_today_context()
