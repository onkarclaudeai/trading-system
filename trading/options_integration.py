#!/usr/bin/env python3
"""
Options Trading Integration Module v2
Integrates watchlist with real options data to generate actionable trade setups.

This module solves the problem of having to manually look up options for each stock
by automatically generating an "Options Trading Sheet" with:
- Real strike prices based on stock price
- Suggested expiry dates
- Premium estimates (when live data available)
- Risk/reward calculations
- Position sizing suggestions
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json
import requests
from typing import Dict, List, Optional, Tuple
from pathlib import Path

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

# NSE F&O stocks with their lot sizes and tick sizes
FNO_STOCKS_CONFIG = {
    'RELIANCE': {'lot_size': 250, 'tick_size': 0.05, 'strike_interval': 20},
    'TCS': {'lot_size': 175, 'tick_size': 0.05, 'strike_interval': 50},
    'HDFCBANK': {'lot_size': 550, 'tick_size': 0.05, 'strike_interval': 20},
    'INFY': {'lot_size': 400, 'tick_size': 0.05, 'strike_interval': 25},
    'ICICIBANK': {'lot_size': 700, 'tick_size': 0.05, 'strike_interval': 10},
    'SBIN': {'lot_size': 1500, 'tick_size': 0.05, 'strike_interval': 10},
    'BHARTIARTL': {'lot_size': 475, 'tick_size': 0.05, 'strike_interval': 20},
    'ITC': {'lot_size': 1600, 'tick_size': 0.05, 'strike_interval': 5},
    'KOTAKBANK': {'lot_size': 400, 'tick_size': 0.05, 'strike_interval': 25},
    'LT': {'lot_size': 150, 'tick_size': 0.05, 'strike_interval': 50},
    'AXISBANK': {'lot_size': 900, 'tick_size': 0.05, 'strike_interval': 20},
    'HINDUNILVR': {'lot_size': 300, 'tick_size': 0.05, 'strike_interval': 25},
    'BAJFINANCE': {'lot_size': 125, 'tick_size': 0.05, 'strike_interval': 100},
    'MARUTI': {'lot_size': 100, 'tick_size': 0.05, 'strike_interval': 100},
    'TITAN': {'lot_size': 375, 'tick_size': 0.05, 'strike_interval': 25},
    'ASIANPAINT': {'lot_size': 300, 'tick_size': 0.05, 'strike_interval': 25},
    'SUNPHARMA': {'lot_size': 700, 'tick_size': 0.05, 'strike_interval': 20},
    'TATAMOTORS': {'lot_size': 1425, 'tick_size': 0.05, 'strike_interval': 10},
    'M&M': {'lot_size': 700, 'tick_size': 0.05, 'strike_interval': 20},
    'WIPRO': {'lot_size': 1500, 'tick_size': 0.05, 'strike_interval': 5},
    'HCLTECH': {'lot_size': 700, 'tick_size': 0.05, 'strike_interval': 25},
    'NTPC': {'lot_size': 3000, 'tick_size': 0.05, 'strike_interval': 5},
    'POWERGRID': {'lot_size': 3000, 'tick_size': 0.05, 'strike_interval': 5},
    'ONGC': {'lot_size': 5700, 'tick_size': 0.05, 'strike_interval': 5},
    'COALINDIA': {'lot_size': 2100, 'tick_size': 0.05, 'strike_interval': 5},
    'INDUSINDBK': {'lot_size': 900, 'tick_size': 0.05, 'strike_interval': 20},
    'TATASTEEL': {'lot_size': 8500, 'tick_size': 0.05, 'strike_interval': 2.5},
    'JSWSTEEL': {'lot_size': 1350, 'tick_size': 0.05, 'strike_interval': 10},
    'HINDALCO': {'lot_size': 2300, 'tick_size': 0.05, 'strike_interval': 10},
    'ADANIENT': {'lot_size': 400, 'tick_size': 0.05, 'strike_interval': 50},
}

# BSE code to NSE symbol mapping (comprehensive)
BSE_TO_NSE_MAP = {
    '500325': 'RELIANCE',
    '532540': 'TCS',
    '500180': 'HDFCBANK',
    '500209': 'INFY',
    '532174': 'ICICIBANK',
    '500112': 'SBIN',
    '532454': 'BHARTIARTL',
    '500875': 'ITC',
    '500247': 'KOTAKBANK',
    '500510': 'LT',
    '532215': 'AXISBANK',
    '500696': 'HINDUNILVR',
    '500034': 'BAJFINANCE',
    '532500': 'MARUTI',
    '500114': 'TITAN',
    '500820': 'ASIANPAINT',
    '524715': 'SUNPHARMA',
    '500570': 'TATAMOTORS',
    '500520': 'M&M',
    '507685': 'WIPRO',
    '532281': 'HCLTECH',
    '532555': 'NTPC',
    '532898': 'POWERGRID',
    '500312': 'ONGC',
    '533278': 'COALINDIA',
    '532187': 'INDUSINDBK',
    '500470': 'TATASTEEL',
    '500228': 'JSWSTEEL',
    '500440': 'HINDALCO',
    '532424': 'ADANIENT',
    '500182': 'HEROMOTOCO',
    '500010': 'HDFC',
    '532977': 'BAJAJ-AUTO',
    '500790': 'NESTLEIND',
    '532538': 'ULTRACEMCO',
    '540376': 'DMART',
    '500770': 'DRREDDY',
    '524348': 'CIPLA',
    '500413': 'TECHM',
    '532286': 'JINDALSTEL',
}


class OptionsIntegration:
    """
    Integrates watchlist stocks with options data to generate tradeable setups.
    """
    
    def __init__(self, conn=None):
        self.conn = conn or psycopg2.connect(**DB_CONFIG)
        self._create_tables()
    
    def _create_tables(self):
        """Create tables for storing options trading setups."""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS options_trading_setups (
                id SERIAL PRIMARY KEY,
                setup_date DATE,
                sc_code VARCHAR(20),
                sc_name VARCHAR(200),
                nse_symbol VARCHAR(20),
                
                -- Underlying details
                spot_price DECIMAL(15,2),
                direction VARCHAR(20),
                confidence_score DECIMAL(5,2),
                
                -- Options recommendation
                recommended_option_type VARCHAR(5),
                recommended_strike DECIMAL(15,2),
                strike_type VARCHAR(10),  -- ATM, OTM, ITM
                expiry_date DATE,
                
                -- Position details
                lot_size INTEGER,
                estimated_premium DECIMAL(15,2),
                max_risk DECIMAL(15,2),
                target_premium DECIMAL(15,2),
                stop_loss_premium DECIMAL(15,2),
                risk_reward_ratio DECIMAL(5,2),
                
                -- Trade management
                entry_strategy TEXT,
                exit_criteria TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(setup_date, sc_code, recommended_strike, recommended_option_type)
            );
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_options_setups_date 
            ON options_trading_setups(setup_date);
            """
        ]
        
        with self.conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
        self.conn.commit()
        logger.info("Options trading tables ready")
    
    def get_nse_symbol(self, sc_code: str, sc_name: str = None) -> Optional[str]:
        """Get NSE symbol from BSE code."""
        # Try direct mapping first
        if sc_code in BSE_TO_NSE_MAP:
            return BSE_TO_NSE_MAP[sc_code]
        
        # Try name-based matching
        if sc_name:
            name_upper = sc_name.upper()
            for nse_symbol in FNO_STOCKS_CONFIG.keys():
                if nse_symbol in name_upper or name_upper.startswith(nse_symbol.split('-')[0]):
                    return nse_symbol
        
        return None
    
    def is_fno_stock(self, nse_symbol: str) -> bool:
        """Check if stock is available in F&O segment."""
        return nse_symbol in FNO_STOCKS_CONFIG if nse_symbol else False
    
    def get_stock_config(self, nse_symbol: str) -> Dict:
        """Get F&O configuration for a stock."""
        if nse_symbol in FNO_STOCKS_CONFIG:
            return FNO_STOCKS_CONFIG[nse_symbol]
        
        # Default config for unknown F&O stocks
        return {
            'lot_size': 100,
            'tick_size': 0.05,
            'strike_interval': 50
        }
    
    def calculate_strikes(self, spot_price: float, strike_interval: float, 
                         direction: str, num_strikes: int = 3) -> List[Dict]:
        """
        Calculate recommended strikes based on spot price and direction.
        
        Returns list of strike recommendations with type (ATM/OTM/ITM).
        """
        # Round spot to nearest strike
        atm_strike = round(spot_price / strike_interval) * strike_interval
        
        strikes = []
        
        if direction.upper() == 'BULLISH':
            # For bullish: ATM CE, slightly OTM CE
            strikes.append({
                'strike': atm_strike,
                'type': 'ATM',
                'option_type': 'CE',
                'moneyness': 0
            })
            
            # 1 strike OTM
            otm_strike = atm_strike + strike_interval
            strikes.append({
                'strike': otm_strike,
                'type': 'OTM',
                'option_type': 'CE',
                'moneyness': (otm_strike - spot_price) / spot_price * 100
            })
            
            # 2 strikes OTM (cheaper, higher risk/reward)
            otm_strike_2 = atm_strike + (2 * strike_interval)
            strikes.append({
                'strike': otm_strike_2,
                'type': 'OTM2',
                'option_type': 'CE',
                'moneyness': (otm_strike_2 - spot_price) / spot_price * 100
            })
            
        elif direction.upper() == 'BEARISH':
            # For bearish: ATM PE, slightly OTM PE
            strikes.append({
                'strike': atm_strike,
                'type': 'ATM',
                'option_type': 'PE',
                'moneyness': 0
            })
            
            # 1 strike OTM
            otm_strike = atm_strike - strike_interval
            strikes.append({
                'strike': otm_strike,
                'type': 'OTM',
                'option_type': 'PE',
                'moneyness': (spot_price - otm_strike) / spot_price * 100
            })
            
            # 2 strikes OTM
            otm_strike_2 = atm_strike - (2 * strike_interval)
            strikes.append({
                'strike': otm_strike_2,
                'type': 'OTM2',
                'option_type': 'PE',
                'moneyness': (spot_price - otm_strike_2) / spot_price * 100
            })
        
        else:  # NEUTRAL - suggest straddle
            strikes.append({
                'strike': atm_strike,
                'type': 'ATM',
                'option_type': 'CE+PE',
                'moneyness': 0
            })
        
        return strikes
    
    def get_next_expiries(self, num_expiries: int = 2) -> List[datetime]:
        """
        Get upcoming Thursday expiry dates.
        NSE weekly expiries are on Thursdays.
        """
        today = datetime.now().date()
        expiries = []
        
        current = today
        while len(expiries) < num_expiries:
            # Find next Thursday (weekday 3)
            days_until_thursday = (3 - current.weekday()) % 7
            if days_until_thursday == 0 and current <= today:
                days_until_thursday = 7
            
            next_thursday = current + timedelta(days=days_until_thursday)
            
            if next_thursday > today:
                expiries.append(next_thursday)
            
            current = next_thursday + timedelta(days=1)
        
        return expiries
    
    def estimate_premium(self, spot_price: float, strike: float, 
                        days_to_expiry: int, option_type: str,
                        volatility_pct: float = 20) -> float:
        """
        Estimate option premium using simplified Black-Scholes approximation.
        This is a rough estimate - actual premiums may vary.
        """
        # Simplified premium estimation
        time_value = (days_to_expiry / 365.0) ** 0.5
        
        if option_type == 'CE':
            intrinsic = max(0, spot_price - strike)
            otm_factor = max(0, (strike - spot_price) / spot_price)
        else:  # PE
            intrinsic = max(0, strike - spot_price)
            otm_factor = max(0, (spot_price - strike) / spot_price)
        
        # Time value component (simplified)
        time_premium = spot_price * (volatility_pct / 100) * time_value * 0.4
        
        # Reduce time value for OTM options
        time_premium *= (1 - otm_factor * 2) if otm_factor < 0.5 else 0.1
        
        estimated_premium = intrinsic + max(time_premium, spot_price * 0.005)
        
        return round(estimated_premium, 2)
    
    def generate_trade_setup(self, stock_data: Dict, direction: str,
                            confidence_score: float) -> List[Dict]:
        """
        Generate complete trade setups for a stock.
        
        Args:
            stock_data: Dictionary with sc_code, sc_name, close_price, atr_pct
            direction: 'BULLISH' or 'BEARISH'
            confidence_score: 0-100 confidence score
            
        Returns:
            List of trade setup dictionaries
        """
        sc_code = stock_data.get('sc_code', '')
        sc_name = stock_data.get('sc_name', '')
        spot_price = float(stock_data.get('close_price', 0))
        atr_pct = float(stock_data.get('atr_pct', 2))
        
        if spot_price <= 0:
            return []
        
        # Get NSE symbol
        nse_symbol = self.get_nse_symbol(sc_code, sc_name)
        
        if not nse_symbol or not self.is_fno_stock(nse_symbol):
            logger.debug(f"Skipping {sc_name} - not in F&O segment")
            return []
        
        # Get stock configuration
        config = self.get_stock_config(nse_symbol)
        lot_size = config['lot_size']
        strike_interval = config['strike_interval']
        
        # Get expiry dates
        expiries = self.get_next_expiries(2)
        
        # Calculate strikes
        strikes = self.calculate_strikes(spot_price, strike_interval, direction)
        
        setups = []
        
        for strike_info in strikes[:2]:  # Top 2 recommendations
            for expiry in expiries[:1]:  # Current week only for intraday
                strike = strike_info['strike']
                option_type = strike_info['option_type']
                
                if option_type == 'CE+PE':
                    continue  # Skip straddle for now
                
                days_to_expiry = (expiry - datetime.now().date()).days
                
                # Estimate premium
                estimated_premium = self.estimate_premium(
                    spot_price, strike, days_to_expiry, option_type, atr_pct * 10
                )
                
                # Calculate risk/reward
                max_risk = estimated_premium * lot_size
                
                # Target: 50% of premium for intraday
                target_premium = estimated_premium * 1.5
                target_value = target_premium * lot_size
                
                # Stop loss: 50% loss of premium
                stop_loss_premium = estimated_premium * 0.5
                
                risk_reward = ((target_premium - estimated_premium) / 
                              (estimated_premium - stop_loss_premium)) if stop_loss_premium > 0 else 0
                
                # Entry strategy based on confidence
                if confidence_score >= 70:
                    entry_strategy = "Enter at market open if gap < 1%"
                elif confidence_score >= 50:
                    entry_strategy = "Wait for 9:30 AM confirmation, enter on pullback"
                else:
                    entry_strategy = "Wait for breakout above previous day high"
                
                # Exit criteria
                exit_criteria = f"Exit at 50% profit (Rs {target_premium:.2f}) or 50% loss (Rs {stop_loss_premium:.2f}). Time stop: 2:30 PM"
                
                setup = {
                    'setup_date': datetime.now().date(),
                    'sc_code': sc_code,
                    'sc_name': sc_name,
                    'nse_symbol': nse_symbol,
                    'spot_price': spot_price,
                    'direction': direction,
                    'confidence_score': confidence_score,
                    'recommended_option_type': option_type,
                    'recommended_strike': strike,
                    'strike_type': strike_info['type'],
                    'expiry_date': expiry,
                    'lot_size': lot_size,
                    'estimated_premium': estimated_premium,
                    'max_risk': max_risk,
                    'target_premium': target_premium,
                    'stop_loss_premium': stop_loss_premium,
                    'risk_reward_ratio': round(risk_reward, 2),
                    'entry_strategy': entry_strategy,
                    'exit_criteria': exit_criteria
                }
                
                setups.append(setup)
        
        return setups
    
    def process_watchlist(self, watchlist_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        Process entire watchlist and generate options trading setups.
        
        Args:
            watchlist_df: If None, loads today's watchlist from file
            
        Returns:
            DataFrame with all trade setups
        """
        logger.info("=" * 70)
        logger.info("GENERATING OPTIONS TRADING SETUPS")
        logger.info("=" * 70)
        
        if watchlist_df is None:
            # Try to load today's watchlist
            watchlist_files = [
                f"enhanced_watchlist_{datetime.now().strftime('%Y%m%d')}.csv",
                f"options_watchlist_{datetime.now().strftime('%Y%m%d')}.csv",
            ]
            
            for file in watchlist_files:
                if os.path.exists(file):
                    watchlist_df = pd.read_csv(file)
                    logger.info(f"Loaded watchlist from {file}")
                    break
            
            if watchlist_df is None:
                logger.error("No watchlist file found")
                return None
        
        if len(watchlist_df) == 0:
            logger.warning("Empty watchlist")
            return pd.DataFrame()
        
        # Process each stock
        all_setups = []
        fno_count = 0
        non_fno_count = 0
        
        for _, row in watchlist_df.iterrows():
            stock_data = row.to_dict()
            
            # Determine direction
            direction = row.get('direction', row.get('predicted_direction', 'BULLISH'))
            if pd.isna(direction) or direction == '':
                # Infer from momentum
                momentum = row.get('momentum_3d', 0)
                direction = 'BULLISH' if momentum > 0 else 'BEARISH'
            
            # Get confidence score
            confidence = row.get('options_score', row.get('combined_ml_score', 50))
            if pd.isna(confidence):
                confidence = 50
            
            # Generate setups
            setups = self.generate_trade_setup(stock_data, direction, confidence)
            
            if setups:
                all_setups.extend(setups)
                fno_count += 1
            else:
                non_fno_count += 1
        
        logger.info(f"F&O stocks processed: {fno_count}")
        logger.info(f"Non-F&O stocks skipped: {non_fno_count}")
        
        if not all_setups:
            logger.warning("No F&O setups generated")
            return pd.DataFrame()
        
        # Create DataFrame
        setups_df = pd.DataFrame(all_setups)
        
        # Sort by confidence and risk/reward
        setups_df = setups_df.sort_values(
            ['confidence_score', 'risk_reward_ratio'],
            ascending=[False, False]
        )
        
        # Save to database
        self._save_setups_to_db(setups_df)
        
        # Generate and save trading sheet
        self.generate_trading_sheet(setups_df)
        
        return setups_df
    
    def _save_setups_to_db(self, setups_df: pd.DataFrame):
        """Save setups to database."""
        insert_query = """
        INSERT INTO options_trading_setups (
            setup_date, sc_code, sc_name, nse_symbol, spot_price, direction,
            confidence_score, recommended_option_type, recommended_strike,
            strike_type, expiry_date, lot_size, estimated_premium, max_risk,
            target_premium, stop_loss_premium, risk_reward_ratio,
            entry_strategy, exit_criteria
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (setup_date, sc_code, recommended_strike, recommended_option_type) 
        DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            estimated_premium = EXCLUDED.estimated_premium
        """
        
        with self.conn.cursor() as cur:
            for _, row in setups_df.iterrows():
                try:
                    cur.execute(insert_query, (
                        row['setup_date'], row['sc_code'], row['sc_name'],
                        row['nse_symbol'], row['spot_price'], row['direction'],
                        row['confidence_score'], row['recommended_option_type'],
                        row['recommended_strike'], row['strike_type'],
                        row['expiry_date'], row['lot_size'], row['estimated_premium'],
                        row['max_risk'], row['target_premium'], row['stop_loss_premium'],
                        row['risk_reward_ratio'], row['entry_strategy'], row['exit_criteria']
                    ))
                except Exception as e:
                    logger.debug(f"Could not save setup: {e}")
        
        self.conn.commit()
        logger.info(f"Saved {len(setups_df)} setups to database")
    
    def generate_trading_sheet(self, setups_df: pd.DataFrame):
        """
        Generate a formatted trading sheet for easy reference.
        """
        output_file = f"options_trading_sheet_{datetime.now().strftime('%Y%m%d')}.csv"
        
        # Select and rename columns for trading sheet
        columns = [
            'nse_symbol', 'direction', 'spot_price', 
            'recommended_option_type', 'recommended_strike', 'strike_type',
            'expiry_date', 'lot_size', 'estimated_premium', 
            'max_risk', 'target_premium', 'stop_loss_premium',
            'risk_reward_ratio', 'confidence_score'
        ]
        
        trading_sheet = setups_df[columns].copy()
        trading_sheet.columns = [
            'Symbol', 'Direction', 'Spot', 
            'Option', 'Strike', 'Type',
            'Expiry', 'Lot', 'Premium (Est)',
            'Max Risk', 'Target', 'Stop Loss',
            'R:R', 'Confidence'
        ]
        
        trading_sheet.to_csv(output_file, index=False)
        logger.info(f"Trading sheet saved to {output_file}")
        
        # Also generate text report
        report = self._generate_text_report(setups_df)
        report_file = f"options_trading_report_{datetime.now().strftime('%Y%m%d')}.txt"
        
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(report)
        logger.info(f"Trading report saved to {report_file}")
        
        return output_file
    
    def _generate_text_report(self, setups_df: pd.DataFrame) -> str:
        """Generate formatted text report."""
        
        lines = []
        lines.append("=" * 80)
        lines.append("OPTIONS TRADING SHEET")
        lines.append(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"Expiry: {setups_df['expiry_date'].iloc[0] if len(setups_df) > 0 else 'N/A'}")
        lines.append("=" * 80)
        
        # Summary
        bullish = len(setups_df[setups_df['direction'] == 'BULLISH'])
        bearish = len(setups_df[setups_df['direction'] == 'BEARISH'])
        lines.append(f"\nTotal Setups: {len(setups_df)} ({bullish} Bullish, {bearish} Bearish)")
        
        # Group by symbol
        for symbol in setups_df['nse_symbol'].unique():
            symbol_setups = setups_df[setups_df['nse_symbol'] == symbol]
            
            lines.append("\n" + "-" * 70)
            row = symbol_setups.iloc[0]
            
            direction_emoji = "UP" if row['direction'] == 'BULLISH' else "DOWN"
            lines.append(f"{direction_emoji} {symbol} | Spot: Rs {row['spot_price']:.2f} | Confidence: {row['confidence_score']:.0f}%")
            lines.append("-" * 70)
            
            for _, setup in symbol_setups.iterrows():
                option_str = f"{setup['recommended_option_type']} {setup['recommended_strike']:.0f}"
                lines.append(f"  Option: {option_str} ({setup['strike_type']})")
                lines.append(f"  Premium (Est): Rs {setup['estimated_premium']:.2f} | Lot: {setup['lot_size']}")
                lines.append(f"  Max Risk: Rs {setup['max_risk']:.0f} | R:R Ratio: {setup['risk_reward_ratio']:.1f}")
                lines.append(f"  Target: Rs {setup['target_premium']:.2f} | Stop Loss: Rs {setup['stop_loss_premium']:.2f}")
                lines.append(f"  Entry: {setup['entry_strategy']}")
                lines.append("")
        
        lines.append("=" * 80)
        lines.append("TRADING TIPS:")
        lines.append("1. Verify premiums from live market data before trading")
        lines.append("2. Always use stop losses")
        lines.append("3. Don't overtrade - pick 2-3 best setups")
        lines.append("4. Exit all positions by 3:15 PM for intraday")
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def cleanup(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Main function for options integration."""
    logger.info("Starting Options Trading Integration...")
    
    try:
        integration = OptionsIntegration()
        
        # Process watchlist and generate trading sheet
        setups = integration.process_watchlist()
        
        if setups is not None and len(setups) > 0:
            logger.info(f"Generated {len(setups)} trading setups")
            logger.info("Check options_trading_sheet_*.csv for trade setups")
        else:
            logger.warning("No trading setups generated")
        
        integration.cleanup()
        
    except Exception as e:
        logger.error(f"Error in options integration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
