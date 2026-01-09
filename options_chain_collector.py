#!/usr/bin/env python3
"""
Options Chain Collector - Sharekhan API Integration
Fetches real options chain data for watchlist stocks.

Requirements:
    pip install shareconnect websocket-client requests

Environment Variables (or pass directly):
    SHAREKHAN_API_KEY - Your Sharekhan API key
    SHAREKHAN_SECRET_KEY - Your Sharekhan secret key
    SHAREKHAN_CUSTOMER_ID - Your Sharekhan customer ID
    SHAREKHAN_ACCESS_TOKEN - (Optional) Cached access token
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# BSE to NSE symbol mapping for F&O stocks
BSE_TO_NSE_SYMBOL = {
    '500325': 'RELIANCE',
    '500180': 'HDFCBANK',
    '532174': 'ICICIBANK',
    '500209': 'INFY',
    '532540': 'TCS',
    '500112': 'SBIN',
    '500510': 'LT',
    '500087': 'CIPLA',
    '532898': 'POWERGRID',
    '500103': 'BHEL',
    '500408': 'TATAELXSI',
    '500188': 'HINDZINC',
    '532540': 'TCS',
    '500034': 'BAJFINANCE',
    '500247': 'KOTAKBANK',
    '532454': 'BHARTIARTL',
    '500696': 'HINDUNILVR',
    '500570': 'TATAMOTORS',
    '500312': 'ONGC',
    '500470': 'TATASTEEL',
    '500182': 'HEROMOTOCO',
    '532500': 'MARUTI',
    '500114': 'TITAN',
    '500124': 'DRREDDY',
    '500010': 'HDFC',
    '500790': 'NESTLEIND',
    '532555': 'NTPC',
    '500875': 'ITC',
    '532286': 'JINDALSTEL',
    '500440': 'HINDALCO',
    '500520': 'MAHINDRA',
    '532978': 'BAJAJFINSV',
    '500820': 'ASIANPAINT',
    '532187': 'TECHM',
    '500680': 'PFIZER',
    '500331': 'PIDILITIND',
    '500295': 'DIVISLAB',
    '500483': 'ULTRACEMCO',
    '532281': 'HDFCLIFE',
    '540115': 'MPHASIS',
    '532215': 'AXISBANK',
    '500696': 'HINDUNILVR',
    '500179': 'HDFCAMC',
    '543320': 'ZOMATO',
    '544262': 'NYKAA',
    '543390': 'POLICYBZR',
}

# F&O enabled stocks on NSE (subset - main liquid ones)
FNO_STOCKS = [
    'RELIANCE', 'HDFCBANK', 'ICICIBANK', 'INFY', 'TCS', 'SBIN', 'LT', 
    'CIPLA', 'POWERGRID', 'BHARTIARTL', 'HINDUNILVR', 'TATAMOTORS',
    'TATASTEEL', 'MARUTI', 'TITAN', 'DRREDDY', 'NTPC', 'ITC',
    'HINDALCO', 'M&M', 'BAJFINANCE', 'ASIANPAINT', 'TECHM', 'AXISBANK',
    'KOTAKBANK', 'BAJAJFINSV', 'HDFCLIFE', 'ONGC', 'ULTRACEMCO', 'ZOMATO'
]


class SharekhanOptionsCollector:
    """Collect options chain data using Sharekhan API."""
    
    def __init__(self, api_key: str = None, secret_key: str = None, 
                 customer_id: str = None, access_token: str = None):
        """
        Initialize Sharekhan API connection.
        
        Args:
            api_key: Sharekhan API key
            secret_key: Sharekhan secret key  
            customer_id: Sharekhan customer ID
            access_token: Pre-existing access token (optional)
        """
        self.api_key = api_key or os.getenv('SHAREKHAN_API_KEY')
        self.secret_key = secret_key or os.getenv('SHAREKHAN_SECRET_KEY')
        self.customer_id = customer_id or os.getenv('SHAREKHAN_CUSTOMER_ID')
        self.access_token = access_token or os.getenv('SHAREKHAN_ACCESS_TOKEN')
        
        self.base_url = "https://api.sharekhan.com"
        self.session = requests.Session()
        self.script_master = {}
        self.is_authenticated = False
        
        if not all([self.api_key, self.secret_key, self.customer_id]):
            logger.warning("Sharekhan credentials not fully configured")
        
    def authenticate(self) -> bool:
        """
        Authenticate with Sharekhan API.
        
        Note: Sharekhan requires manual login flow for the first time.
        After that, you can cache the access_token.
        
        Returns:
            True if authenticated, False otherwise
        """
        if self.access_token:
            # Validate existing token
            self.session.headers.update({
                'Api-Key': self.api_key,
                'Access-Token': self.access_token,
                'Content-Type': 'application/json'
            })
            self.is_authenticated = True
            logger.info("Using existing access token")
            return True
        
        logger.warning("""
        ================================================================
        SHAREKHAN API MANUAL LOGIN REQUIRED
        ================================================================
        
        Sharekhan requires a one-time manual login to get access token.
        
        Steps:
        1. Run: python options_chain_collector.py --login
        2. Open the URL in browser and login
        3. Copy the request_token from redirect URL
        4. Run: python options_chain_collector.py --token YOUR_REQUEST_TOKEN
        5. Save the access_token to environment variable
        
        After first login, set SHAREKHAN_ACCESS_TOKEN env variable
        to skip this step in future runs.
        ================================================================
        """)
        return False
    
    def get_login_url(self) -> str:
        """Get the login URL for manual authentication."""
        try:
            from SharekhanApi.sharekhanConnect import SharekhanConnect
            login = SharekhanConnect(self.api_key)
            url = login.login_url(vendor_key="", version_id="")
            return url
        except ImportError:
            # Fallback URL construction
            return f"https://api.sharekhan.com/oapi/auth/login?api_key={self.api_key}"
    
    def generate_access_token(self, request_token: str) -> str:
        """
        Generate access token from request token.
        
        Args:
            request_token: Token received after manual login
            
        Returns:
            Access token string
        """
        try:
            from SharekhanApi.sharekhanConnect import SharekhanConnect
            login = SharekhanConnect(self.api_key)
            session = login.generate_session_without_versionId(request_token, self.secret_key)
            access_token = login.get_access_token(self.api_key, session, "12345")
            
            self.access_token = access_token
            self.is_authenticated = True
            
            logger.info(f"Access token generated successfully")
            logger.info(f"Save this to SHAREKHAN_ACCESS_TOKEN: {access_token}")
            
            return access_token
            
        except Exception as e:
            logger.error(f"Failed to generate access token: {e}")
            return None
    
    def load_script_master(self, exchange: str = "NF") -> Dict:
        """
        Load script master for F&O segment.
        
        Args:
            exchange: Exchange code (NF = NSE F&O)
            
        Returns:
            Dictionary of scripts
        """
        try:
            from SharekhanApi.sharekhanConnect import SharekhanConnect
            sharekhan = SharekhanConnect(self.api_key, self.access_token)
            
            master_data = sharekhan.master(exchange)
            
            if master_data:
                self.script_master[exchange] = master_data
                logger.info(f"Loaded {len(master_data)} scripts for {exchange}")
                
            return master_data
            
        except Exception as e:
            logger.error(f"Failed to load script master: {e}")
            return {}
    
    def get_expiry_dates(self, symbol: str) -> List[str]:
        """
        Get available expiry dates for a symbol.
        
        Args:
            symbol: NSE trading symbol
            
        Returns:
            List of expiry dates in DD/MM/YYYY format
        """
        # Calculate upcoming monthly expiries (last Thursday of month)
        expiries = []
        today = datetime.now()
        
        for i in range(3):  # Next 3 monthly expiries
            # Get last Thursday of month
            month = today.month + i
            year = today.year
            if month > 12:
                month -= 12
                year += 1
            
            # Find last Thursday
            import calendar
            cal = calendar.monthcalendar(year, month)
            # Last Thursday is in last week that has Thursday (index 3)
            for week in reversed(cal):
                if week[3] != 0:  # Thursday is index 3
                    last_thursday = week[3]
                    break
            
            expiry_date = datetime(year, month, last_thursday)
            if expiry_date > today:
                expiries.append(expiry_date.strftime("%d/%m/%Y"))
        
        return expiries
    
    def get_strike_prices(self, symbol: str, spot_price: float, 
                          num_strikes: int = 5) -> Dict[str, List[float]]:
        """
        Calculate strike prices around the spot price.
        
        Args:
            symbol: Trading symbol
            spot_price: Current spot price
            num_strikes: Number of strikes above and below ATM
            
        Returns:
            Dictionary with 'calls' and 'puts' strike lists
        """
        # Determine strike interval based on price
        if spot_price < 100:
            interval = 2.5
        elif spot_price < 250:
            interval = 5
        elif spot_price < 500:
            interval = 10
        elif spot_price < 1000:
            interval = 20
        elif spot_price < 2500:
            interval = 50
        elif spot_price < 5000:
            interval = 100
        else:
            interval = 100
        
        # Find ATM strike
        atm_strike = round(spot_price / interval) * interval
        
        # Generate strikes
        strikes = []
        for i in range(-num_strikes, num_strikes + 1):
            strike = atm_strike + (i * interval)
            if strike > 0:
                strikes.append(strike)
        
        return {
            'atm': atm_strike,
            'interval': interval,
            'strikes': sorted(strikes),
            'itm_calls': [s for s in strikes if s < spot_price],
            'otm_calls': [s for s in strikes if s >= spot_price],
            'itm_puts': [s for s in strikes if s > spot_price],
            'otm_puts': [s for s in strikes if s <= spot_price]
        }
    
    def fetch_option_quote(self, symbol: str, strike: float, 
                           option_type: str, expiry: str) -> Dict:
        """
        Fetch quote for a specific option contract.
        
        Args:
            symbol: Trading symbol
            strike: Strike price
            option_type: 'CE' or 'PE'
            expiry: Expiry date in DD/MM/YYYY format
            
        Returns:
            Option quote data
        """
        try:
            from SharekhanApi.sharekhanConnect import SharekhanConnect
            sharekhan = SharekhanConnect(self.api_key, self.access_token)
            
            # Construct option params for quote
            # This depends on Sharekhan's specific API structure
            quote_params = {
                "exchange": "NF",
                "tradingSymbol": symbol,
                "instrumentType": "OS",  # Option Stocks
                "strikePrice": str(strike),
                "optionType": option_type,
                "expiry": expiry
            }
            
            # Note: Actual implementation depends on Sharekhan's quote API
            # This is a placeholder structure
            return quote_params
            
        except Exception as e:
            logger.error(f"Failed to fetch quote for {symbol} {strike} {option_type}: {e}")
            return {}
    
    def get_options_chain(self, symbol: str, spot_price: float, 
                          expiry: str = None) -> pd.DataFrame:
        """
        Get full options chain for a symbol.
        
        Args:
            symbol: NSE trading symbol
            spot_price: Current spot price
            expiry: Expiry date (optional, uses nearest if not provided)
            
        Returns:
            DataFrame with options chain data
        """
        if not expiry:
            expiries = self.get_expiry_dates(symbol)
            expiry = expiries[0] if expiries else None
        
        if not expiry:
            logger.error(f"Could not determine expiry for {symbol}")
            return pd.DataFrame()
        
        # Get strikes
        strike_info = self.get_strike_prices(symbol, spot_price)
        
        options_data = []
        
        for strike in strike_info['strikes']:
            # Estimate premiums (simplified - real implementation would fetch live)
            # Using Black-Scholes approximation
            time_to_expiry = self._get_time_to_expiry(expiry)
            iv = 0.25  # Assumed IV of 25%
            
            call_premium = self._estimate_premium(spot_price, strike, time_to_expiry, iv, 'CE')
            put_premium = self._estimate_premium(spot_price, strike, time_to_expiry, iv, 'PE')
            
            options_data.append({
                'symbol': symbol,
                'expiry': expiry,
                'strike': strike,
                'call_premium': call_premium,
                'put_premium': put_premium,
                'call_iv': iv * 100,
                'put_iv': iv * 100,
                'is_atm': strike == strike_info['atm'],
                'moneyness': 'ATM' if strike == strike_info['atm'] else 
                            ('ITM' if strike < spot_price else 'OTM')
            })
        
        return pd.DataFrame(options_data)
    
    def _get_time_to_expiry(self, expiry: str) -> float:
        """Calculate time to expiry in years."""
        try:
            expiry_date = datetime.strptime(expiry, "%d/%m/%Y")
            today = datetime.now()
            days = (expiry_date - today).days
            return max(days / 365, 0.001)
        except:
            return 0.05  # Default ~18 days
    
    def _estimate_premium(self, spot: float, strike: float, 
                          time: float, iv: float, option_type: str) -> float:
        """
        Estimate option premium using simplified Black-Scholes.
        
        This is an approximation - real implementation would fetch live prices.
        """
        import math
        
        r = 0.07  # Risk-free rate ~7%
        
        try:
            d1 = (math.log(spot / strike) + (r + 0.5 * iv**2) * time) / (iv * math.sqrt(time))
            d2 = d1 - iv * math.sqrt(time)
            
            # Simplified normal CDF approximation
            def norm_cdf(x):
                return 0.5 * (1 + math.erf(x / math.sqrt(2)))
            
            if option_type == 'CE':
                premium = spot * norm_cdf(d1) - strike * math.exp(-r * time) * norm_cdf(d2)
            else:  # PE
                premium = strike * math.exp(-r * time) * norm_cdf(-d2) - spot * norm_cdf(-d1)
            
            return round(max(premium, 0.05), 2)
            
        except:
            # Fallback: intrinsic value + small time value
            if option_type == 'CE':
                intrinsic = max(spot - strike, 0)
            else:
                intrinsic = max(strike - spot, 0)
            
            return round(intrinsic + spot * 0.01, 2)
    
    def get_trading_recommendations(self, watchlist: pd.DataFrame) -> pd.DataFrame:
        """
        Generate options trading recommendations for watchlist stocks.
        
        Args:
            watchlist: DataFrame with watchlist stocks
            
        Returns:
            DataFrame with options recommendations
        """
        recommendations = []
        
        for _, row in watchlist.iterrows():
            sc_code = str(row.get('sc_code', ''))
            nse_symbol = BSE_TO_NSE_SYMBOL.get(sc_code)
            
            if not nse_symbol or nse_symbol not in FNO_STOCKS:
                # Not an F&O stock
                recommendations.append({
                    'sc_code': sc_code,
                    'sc_name': row.get('sc_name', ''),
                    'nse_symbol': nse_symbol or 'N/A',
                    'has_options': False,
                    'recommendation': 'No F&O available',
                    'strike': None,
                    'option_type': None,
                    'estimated_premium': None,
                    'expiry': None
                })
                continue
            
            spot_price = float(row.get('close_price', row.get('close', 0)))
            direction = str(row.get('direction', row.get('predicted_direction', 'NEUTRAL'))).upper()
            
            if spot_price <= 0:
                continue
            
            # Get strike info
            strike_info = self.get_strike_prices(nse_symbol, spot_price)
            expiries = self.get_expiry_dates(nse_symbol)
            expiry = expiries[0] if expiries else 'N/A'
            
            # Determine option type based on direction
            if 'BULL' in direction or direction == 'BUY':
                option_type = 'CE'
                # ATM or slightly OTM call
                strike = strike_info['atm']
                strategy = 'BUY CALL'
            elif 'BEAR' in direction or direction == 'SELL':
                option_type = 'PE'
                # ATM or slightly OTM put
                strike = strike_info['atm']
                strategy = 'BUY PUT'
            else:
                option_type = 'STRADDLE'
                strike = strike_info['atm']
                strategy = 'STRADDLE/STRANGLE'
            
            # Estimate premium
            time_to_expiry = self._get_time_to_expiry(expiry) if expiry != 'N/A' else 0.05
            iv = 0.25 + (float(row.get('atr_pct', 2)) / 100)  # Base IV + volatility adjustment
            
            if option_type in ['CE', 'PE']:
                premium = self._estimate_premium(spot_price, strike, time_to_expiry, iv, option_type)
            else:
                call_prem = self._estimate_premium(spot_price, strike, time_to_expiry, iv, 'CE')
                put_prem = self._estimate_premium(spot_price, strike, time_to_expiry, iv, 'PE')
                premium = call_prem + put_prem
            
            # Calculate lot size and capital required
            lot_size = self._get_lot_size(nse_symbol)
            capital_required = premium * lot_size
            
            recommendations.append({
                'sc_code': sc_code,
                'sc_name': row.get('sc_name', ''),
                'nse_symbol': nse_symbol,
                'spot_price': spot_price,
                'has_options': True,
                'direction': direction,
                'recommendation': strategy,
                'strike': strike,
                'option_type': option_type,
                'expiry': expiry,
                'estimated_premium': premium,
                'lot_size': lot_size,
                'capital_required': capital_required,
                'atm_strike': strike_info['atm'],
                'strike_interval': strike_info['interval']
            })
        
        return pd.DataFrame(recommendations)
    
    def _get_lot_size(self, symbol: str) -> int:
        """Get lot size for an F&O stock."""
        # Common lot sizes (as of 2024 - these change periodically)
        lot_sizes = {
            'RELIANCE': 250,
            'HDFCBANK': 550,
            'ICICIBANK': 700,
            'INFY': 300,
            'TCS': 150,
            'SBIN': 1500,
            'LT': 150,
            'CIPLA': 650,
            'POWERGRID': 2700,
            'BHARTIARTL': 475,
            'HINDUNILVR': 300,
            'TATAMOTORS': 1425,
            'TATASTEEL': 5500,
            'MARUTI': 100,
            'TITAN': 375,
            'DRREDDY': 125,
            'NTPC': 2850,
            'ITC': 1600,
            'HINDALCO': 1075,
            'M&M': 350,
            'BAJFINANCE': 125,
            'ASIANPAINT': 300,
            'TECHM': 600,
            'AXISBANK': 1200,
            'KOTAKBANK': 400,
            'BAJAJFINSV': 500,
            'HDFCLIFE': 1100,
            'ONGC': 3850,
            'ULTRACEMCO': 100,
            'ZOMATO': 6000,
        }
        return lot_sizes.get(symbol, 500)  # Default lot size


def generate_options_setups(watchlist_file: str = None) -> pd.DataFrame:
    """
    Generate options trading setups from watchlist.
    
    This is the main entry point called by daily_flow_orchestrator.
    
    Args:
        watchlist_file: Path to watchlist CSV file
        
    Returns:
        DataFrame with options recommendations
    """
    # Find watchlist file
    if not watchlist_file:
        today = datetime.now().strftime('%Y%m%d')
        candidates = [
            f'options_watchlist_{today}.csv',
            f'enhanced_watchlist_{today}.csv',
            f'watchlist_{today}.csv'
        ]
        for f in candidates:
            if os.path.exists(f):
                watchlist_file = f
                break
    
    if not watchlist_file or not os.path.exists(watchlist_file):
        logger.warning("No watchlist file found")
        return pd.DataFrame()
    
    # Load watchlist
    watchlist = pd.read_csv(watchlist_file)
    watchlist.columns = watchlist.columns.str.lower().str.replace(' ', '_')
    
    logger.info(f"Processing {len(watchlist)} stocks from {watchlist_file}")
    
    # Initialize collector
    collector = SharekhanOptionsCollector()
    
    # Check if we have credentials for live data
    if collector.api_key and collector.access_token:
        if collector.authenticate():
            logger.info("Using live Sharekhan data")
    else:
        logger.info("Using estimated options data (no Sharekhan credentials)")
    
    # Generate recommendations
    recommendations = collector.get_trading_recommendations(watchlist)
    
    # Filter to only F&O stocks
    fno_recs = recommendations[recommendations['has_options'] == True]
    
    if len(fno_recs) > 0:
        # Save to file
        output_file = f'options_setups_{datetime.now().strftime("%Y%m%d")}.csv'
        fno_recs.to_csv(output_file, index=False)
        logger.info(f"Saved {len(fno_recs)} options setups to {output_file}")
        
        # Print summary
        print("\n" + "=" * 80)
        print("OPTIONS TRADING SETUPS")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("=" * 80)
        
        for _, row in fno_recs.head(10).iterrows():
            print(f"\n{row['nse_symbol']} ({row['sc_code']})")
            print(f"  Spot: ₹{row['spot_price']:.2f}")
            print(f"  Strategy: {row['recommendation']}")
            print(f"  Strike: ₹{row['strike']:.0f} {row['option_type']}")
            print(f"  Expiry: {row['expiry']}")
            print(f"  Est. Premium: ₹{row['estimated_premium']:.2f}")
            print(f"  Lot Size: {row['lot_size']}")
            print(f"  Capital Required: ₹{row['capital_required']:,.0f}")
        
        print("\n" + "=" * 80)
    else:
        logger.info("No F&O stocks in today's watchlist")
    
    return recommendations


def main():
    """Main function with CLI support."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Sharekhan Options Chain Collector')
    parser.add_argument('--login', action='store_true', help='Get login URL')
    parser.add_argument('--token', type=str, help='Generate access token from request token')
    parser.add_argument('--watchlist', type=str, help='Path to watchlist CSV')
    parser.add_argument('--test', action='store_true', help='Run test with sample data')
    
    args = parser.parse_args()
    
    collector = SharekhanOptionsCollector()
    
    if args.login:
        url = collector.get_login_url()
        print(f"\nLogin URL:\n{url}\n")
        print("After login, copy the request_token from the redirect URL")
        print("Then run: python options_chain_collector.py --token YOUR_REQUEST_TOKEN")
        return
    
    if args.token:
        access_token = collector.generate_access_token(args.token)
        if access_token:
            print(f"\nSuccess! Add this to your environment:")
            print(f"export SHAREKHAN_ACCESS_TOKEN='{access_token}'")
        return
    
    if args.test:
        # Test with sample data
        test_data = pd.DataFrame([
            {'sc_code': '500325', 'sc_name': 'RELIANCE', 'close_price': 1470, 'direction': 'BEARISH'},
            {'sc_code': '500180', 'sc_name': 'HDFCBANK', 'close_price': 946, 'direction': 'NEUTRAL'},
            {'sc_code': '532174', 'sc_name': 'ICICIBANK', 'close_price': 1435, 'direction': 'BULLISH'},
        ])
        recs = collector.get_trading_recommendations(test_data)
        print(recs.to_string())
        return
    
    # Default: process watchlist
    generate_options_setups(args.watchlist)


if __name__ == "__main__":
    main()
