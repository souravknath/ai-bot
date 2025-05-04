import requests
import json
import time
import os
import logging
import csv
from datetime import datetime, timedelta
from dotenv import load_dotenv
from db_handler import DatabaseHandler

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

class StockFetcher:
    def __init__(self):
        """Initialize the StockFetcher with API details"""
        self.api_base_url = "https://api.dhan.co/v2"
        
        # Get API key from environment variable
        self.api_key = os.getenv("DHAN_API_KEY")
        if not self.api_key:
            logging.warning("API key not found in environment variables. Set DHAN_API_KEY in .env file.")
        
        # Headers for API requests
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.api_key
        }
        
        # Exchange segments and instruments we're interested in
        self.exchange_segments = ["NSE_EQ"]  # Can add more like "BSE_EQ", "NSE_FNO", etc.
        self.instrument_types = ["EQUITY"]   # Can add more like "FUTURES", "OPTION", etc.
        
        # Database connection
        self.db = DatabaseHandler()
        self.db.connect()
    
    def fetch_stock_list(self, max_stocks=2000):
        """
        Fetch list of stocks directly from the database
        
        Returns the stocks from the database, limited by max_stocks
        """
        try:
            # Make sure we have a DB connection
            if not self.db.conn or not self.db.cursor:
                self.db.connect()
                
            # Query the database for all stocks
            self.db.cursor.execute(
                "SELECT security_id, exchange_segment, symbol, name, instrument FROM stocks LIMIT ?", 
                (max_stocks,)
            )
            results = self.db.cursor.fetchall()
            
            nse_stocks = []
            for row in results:
                security_id, exchange_segment, symbol, name, instrument = row
                
                stock_data = {
                    "security_id": str(security_id),
                    "symbol": symbol,
                    "name": name if name else symbol,
                    "exchange_segment": exchange_segment,
                    "instrument": instrument if instrument else "EQUITY"
                }
                nse_stocks.append(stock_data)
            
            logging.info(f"Loaded {len(nse_stocks)} stocks from database")
            
            if len(nse_stocks) > 0:
                return nse_stocks
                
            # If no stocks found in database, fall back to hardcoded list
            logging.warning("No stocks found in database, using hardcoded stock list as fallback")
            # Using updated security IDs
            nse_stocks = [
                {"security_id": "INE009A01021", "symbol": "RELIANCE", "name": "Reliance Industries", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE585B01010", "symbol": "MARUTI", "name": "Maruti Suzuki", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE040A01034", "symbol": "HDFCBANK", "name": "HDFC Bank", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE376G01013", "symbol": "BIOCON", "name": "Biocon", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE154A01025", "symbol": "ITC", "name": "ITC", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE885A01032", "symbol": "ULTRACEMCO", "name": "UltraTech Cement", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
            ]
            
            logging.info(f"Using {len(nse_stocks)} hardcoded NSE stocks")
            return nse_stocks[:max_stocks]
            
        except Exception as e:
            logging.error(f"Error loading stocks from database: {e}")
            
            # Fall back to hardcoded list if database loading fails
            logging.warning("Using hardcoded stock list as fallback due to database error")
            nse_stocks = [
                {"security_id": "INE009A01021", "symbol": "RELIANCE", "name": "Reliance Industries", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE585B01010", "symbol": "MARUTI", "name": "Maruti Suzuki", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE040A01034", "symbol": "HDFCBANK", "name": "HDFC Bank", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE376G01013", "symbol": "BIOCON", "name": "Biocon", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE154A01025", "symbol": "ITC", "name": "ITC", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
                {"security_id": "INE885A01032", "symbol": "ULTRACEMCO", "name": "UltraTech Cement", "exchange_segment": "NSE_EQ", "instrument": "EQUITY"},
            ]
            
            logging.info(f"Using {len(nse_stocks)} hardcoded NSE stocks")
            return nse_stocks[:max_stocks]
    
    def fetch_historical_daily_data(self, security_id, exchange_segment, instrument, from_date, to_date, retries=3):
        """Fetch daily historical data for a stock"""
        endpoint = f"{self.api_base_url}/charts/historical"
        
        # Ensure security_id is a string
        security_id = str(security_id)
        
        # Check if this security ID exists in the database and get the correct parameters
        correct_params = self._get_stock_params_from_db(security_id)
        if correct_params:
            # Use the parameters from the database
            exchange_segment = correct_params["exchange_segment"]
            instrument = correct_params["instrument"]
            logging.info(f"Using database parameters for {security_id}: {exchange_segment}, {instrument}")
        else:
            # Ensure exchange_segment and instrument are valid
            if exchange_segment not in ["NSE_EQ", "BSE_EQ", "NSE_FNO"]:
                exchange_segment = "NSE_EQ"  # Default to NSE_EQ if invalid
                logging.warning(f"Invalid exchange_segment for {security_id}. Using default NSE_EQ")
            
            if instrument not in ["EQUITY", "FUTURES", "OPTION"]:
                instrument = "EQUITY"  # Default to EQUITY if invalid
                logging.warning(f"Invalid instrument for {security_id}. Using default EQUITY")
        
        # Format dates properly to ensure they're in YYYY-MM-DD format
        try:
            from_date_obj = datetime.strptime(from_date, "%Y-%m-%d")
            to_date_obj = datetime.strptime(to_date, "%Y-%m-%d")
            from_date = from_date_obj.strftime("%Y-%m-%d")
            to_date = to_date_obj.strftime("%Y-%m-%d")
        except ValueError:
            logging.error(f"Invalid date format for {security_id}: {from_date} to {to_date}")
            # Continue with original values if parsing fails
        
        payload = {
            "securityId": security_id,
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "fromDate": from_date,
            "toDate": to_date
        }
        
        # Add optional fields only if needed
        if instrument in ["FUTURES", "OPTION"]:
            payload["oi"] = True  # Include open interest for derivatives
        
        for attempt in range(retries):
            try:
                logging.info(f"Fetching data for security ID: {security_id}")
                response = requests.post(endpoint, headers=self.headers, json=payload)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit
                    wait_time = (2 ** attempt) + 1  # Exponential backoff
                    logging.warning(f"Rate limit hit. Waiting for {wait_time} seconds before retry.")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Error fetching data: {response.status_code} - {response.text}")
                    
                    # If we get a 400 error with "Missing required fields", try with a simplified payload
                    if response.status_code == 400 and "Missing required fields" in response.text:
                        logging.info(f"Trying simplified payload for {security_id}")
                        # Simplified payload with only essential fields
                        simple_payload = {
                            "securityId": security_id,
                            "exchangeSegment": "NSE_EQ",  # Force NSE_EQ
                            "instrument": "EQUITY",       # Force EQUITY
                            "fromDate": from_date,
                            "toDate": to_date
                        }
                        
                        try:
                            response = requests.post(endpoint, headers=self.headers, json=simple_payload)
                            if response.status_code == 200:
                                return response.json()
                        except Exception as inner_e:
                            logging.error(f"Error with simplified payload: {inner_e}")
                    
                    if attempt < retries - 1:
                        time.sleep(2)  # Wait before retry
                    else:
                        # Try to load demo data if API call fails
                        logging.info(f"Falling back to demo data for {security_id}")
                        demo_data = self.get_demo_data(security_id, from_date, to_date)
                        if demo_data:
                            return demo_data
                        return None
            except Exception as e:
                logging.error(f"Exception while fetching data: {e}")
                if attempt < retries - 1:
                    time.sleep(2)  # Wait before retry
                else:
                    # Try to load demo data if API call fails
                    logging.info(f"Falling back to demo data for {security_id}")
                    demo_data = self.get_demo_data(security_id, from_date, to_date)
                    if demo_data:
                        return demo_data
                    return None
        
        return None
    
    def _get_stock_params_from_db(self, security_id):
        """Get the correct parameters for a security ID from the database"""
        try:
            # Make sure we have a DB connection
            if not self.db.conn or not self.db.cursor:
                self.db.connect()
                
            # Query the database for the security ID
            self.db.cursor.execute(
                "SELECT exchange_segment, instrument FROM stocks WHERE security_id = ?", 
                (security_id,)
            )
            result = self.db.cursor.fetchone()
            
            if result:
                return {
                    "exchange_segment": result[0],
                    "instrument": result[1]
                }
            return None
        except Exception as e:
            logging.error(f"Error querying database for security ID {security_id}: {e}")
            return None
    
    def get_demo_data(self, security_id, from_date, to_date):
        """Generate demo data if API call fails - helpful for testing the UI"""
        try:
            import pandas as pd
            import numpy as np
            from datetime import datetime, timedelta
            
            # Parse dates
            start_date = datetime.strptime(from_date, "%Y-%m-%d")
            end_date = datetime.strptime(to_date, "%Y-%m-%d")
            
            # Create date range for business days only
            date_range = []
            current_date = start_date
            while current_date <= end_date:
                if current_date.weekday() < 5:  # Monday to Friday
                    date_range.append(current_date.strftime("%Y-%m-%d"))
                current_date += timedelta(days=1)
            
            if not date_range:
                return None
                
            # Generate random data with some trend to look realistic
            n_days = len(date_range)
            base_price = 100 + (hash(security_id) % 1000)  # Use hash of security_id for varied starting points
            
            # Generate trending prices
            trend = np.cumsum(np.random.normal(0, 1, n_days)) * 0.5
            
            # Generate OHLC data
            candles = []
            prev_close = base_price
            
            for i, date in enumerate(date_range):
                daily_volatility = max(0.005 * prev_close, 1.0)
                open_price = prev_close * (1 + np.random.normal(0, 0.01))
                
                # Add trend component
                price_with_trend = open_price * (1 + 0.01 * trend[i])
                
                high_price = price_with_trend * (1 + abs(np.random.normal(0, 0.01)))
                low_price = price_with_trend * (1 - abs(np.random.normal(0, 0.01)))
                close_price = (high_price + low_price) / 2 + np.random.normal(0, daily_volatility)
                
                # Ensure OHLC relationships are maintained
                high_price = max(high_price, open_price, close_price)
                low_price = min(low_price, open_price, close_price)
                
                # Generate volume (higher on bigger price moves)
                volume = int(abs(close_price - open_price) * 10000 + np.random.normal(50000, 10000))
                volume = max(volume, 1000)  # Ensure minimum volume
                
                # Round values to 2 decimal places
                open_price = round(open_price, 2)
                high_price = round(high_price, 2)
                low_price = round(low_price, 2)
                close_price = round(close_price, 2)
                
                candle = [date, open_price, high_price, low_price, close_price, volume]
                candles.append(candle)
                
                prev_close = close_price
                
            return {"candles": candles, "status": "success"}
            
        except Exception as e:
            logging.error(f"Error generating demo data: {e}")
            return None
    
    def fetch_historical_data_for_last_year(self, security_id, exchange_segment, instrument):
        """Fetch one year of historical daily data for a stock"""
        # Calculate date range for last 1 year (up to yesterday)
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=365)
        
        from_date = start_date.strftime("%Y-%m-%d")
        to_date = end_date.strftime("%Y-%m-%d")
        
        logging.info(f"Fetching data for {security_id} from {from_date} to {to_date}")
        
        # Fetch the data
        return self.fetch_historical_daily_data(security_id, exchange_segment, instrument, from_date, to_date)