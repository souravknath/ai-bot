import os
import time
import logging
from tqdm import tqdm
from datetime import datetime
from db_handler import DatabaseHandler
from stock_fetcher import StockFetcher
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

def create_dot_env_if_not_exists():
    """Create .env file if it doesn't exist"""
    if not os.path.exists(".env"):
        with open(".env", "w") as f:
            f.write("# Dhan API Key\nDHAN_API_KEY=your_api_key_here\n")
        logging.info("Created .env file. Please add your Dhan API key.")
        return False
    return True

def clean_old_history_data(days=180):
    """Clean historical data older than specified days"""
    logging.info(f"Cleaning historical data older than {days} days...")
    
    # Initialize database handler
    db = DatabaseHandler()
    if not db.connect():
        return False
    
    try:
        # Clean data
        records_deleted = db.clean_history_data(older_than_days=days)
        logging.info(f"Cleaned {records_deleted} historical data records")
        return True
    except Exception as e:
        logging.error(f"Error cleaning historical data: {e}")
        return False
    finally:
        db.close()

def fetch_and_store_stock_data():
    """Main function to fetch and store stock data"""
    # Check if .env file exists and contains API key
    if not create_dot_env_if_not_exists():
        logging.error("Please add your Dhan API key to the .env file and run the script again.")
        return
    
    # Initialize database handler
    db = DatabaseHandler()
    if not db.connect():
        return
    
    # Create tables if they don't exist
    if not db.create_tables():
        db.close()
        return
    
    # Initialize stock fetcher
    fetcher = StockFetcher()
    
    # Check if API key is set
    if not fetcher.api_key:
        logging.error("API key not set. Please add your Dhan API key to the .env file.")
        db.close()
        return
    
    # Fetch the list of stocks
    stocks = fetcher.fetch_stock_list(max_stocks=2000)
    if not stocks:
        logging.error("Failed to fetch stock list.")
        db.close()
        return
    
    logging.info(f"Fetching historical data for {len(stocks)} stocks...")
    
    # Process each stock
    success_count = 0
    
    for stock in tqdm(stocks, desc="Fetching stock data"):
        # Insert stock into database
        stock_id = db.insert_stock(
            stock["security_id"], 
            stock["exchange_segment"], 
            stock["symbol"], 
            stock["name"], 
            stock["instrument"]
        )
        
        if not stock_id:
            logging.error(f"Failed to insert stock {stock['symbol']} into database.")
            continue
        
        # Fetch historical data
        hist_data = fetcher.fetch_historical_data_for_last_year(
            stock["security_id"], 
            stock["exchange_segment"], 
            stock["instrument"]
        )
        
        if not hist_data:
            logging.error(f"Failed to fetch historical data for {stock['symbol']}.")
            continue
        
        # Insert historical data
        inserted_count = db.insert_history_data(stock_id, hist_data)
        
        if inserted_count > 0:
            logging.info(f"Inserted {inserted_count} historical data points for {stock['symbol']}")
            success_count += 1
        else:
            logging.error(f"Failed to insert historical data for {stock['symbol']}")
        
        # Sleep to avoid hitting rate limits
        time.sleep(1)
    
    logging.info(f"Successfully fetched and stored data for {success_count} out of {len(stocks)} stocks")
    db.close()

if __name__ == "__main__":
    start_time = datetime.now()
    logging.info(f"Starting data fetch at {start_time}")
    
    # Clean old history data (older than 180 days by default)
    clean_old_history_data()
    
    fetch_and_store_stock_data()
    
    end_time = datetime.now()
    logging.info(f"Completed in {end_time - start_time}")