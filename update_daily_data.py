import os
import time
import logging
import sys
import schedule
from tqdm import tqdm
from datetime import datetime, timedelta
from db_handler import DatabaseHandler
from stock_fetcher import StockFetcher
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"stock_update_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

def update_latest_stock_data():
    """Update the database with the latest stock data for all stocks"""
    # Initialize database handler
    db = DatabaseHandler()
    if not db.connect():
        logging.error("Failed to connect to the database.")
        return
    
    # Initialize stock fetcher
    fetcher = StockFetcher()
    
    # Check if API key is set
    if not fetcher.api_key:
        logging.error("API key not set. Please add your Dhan API key to the .env file.")
        db.close()
        return
    
    # Get the list of stocks from the database
    stocks = db.get_all_stocks()
    if not stocks:
        logging.error("No stocks found in the database. Please run main.py first.")
        db.close()
        return
    
    logging.info(f"Found {len(stocks)} stocks in the database")

    # Calculate yesterday's date as we want the latest complete day of data
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Calculate date 7 days ago to get a week's worth of data
    # This helps ensure we don't miss any data in case the script wasn't run daily
    week_ago = yesterday - timedelta(days=7)
    week_ago_str = week_ago.strftime("%Y-%m-%d")
    
    logging.info(f"Fetching data from {week_ago_str} to {yesterday_str}")
    
    # Process each stock
    success_count = 0
    skipped_count = 0
    failed_stocks = []
    
    for stock in tqdm(stocks, desc="Updating stock data"):
        stock_id, security_id, exchange_segment, symbol, instrument = stock
        
        # Fetch the latest data (last 7 days to ensure we don't miss any)
        retry_count = 0
        max_retries = 3
        hist_data = None
        
        while retry_count < max_retries and hist_data is None:
            try:
                hist_data = fetcher.fetch_historical_daily_data(
                    security_id, 
                    exchange_segment, 
                    instrument, 
                    week_ago_str,
                    yesterday_str
                )
                
                if not hist_data:
                    retry_count += 1
                    if retry_count < max_retries:
                        logging.warning(f"Retry {retry_count}/{max_retries} for {symbol}")
                        time.sleep(2 ** retry_count)  # Exponential backoff
                    else:
                        logging.error(f"Failed to fetch latest data for {symbol} after {max_retries} attempts")
                        failed_stocks.append(symbol)
            except Exception as e:
                retry_count += 1
                logging.error(f"Error fetching data for {symbol}: {e}")
                if retry_count < max_retries:
                    logging.warning(f"Retry {retry_count}/{max_retries} for {symbol}")
                    time.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    logging.error(f"Failed to fetch latest data for {symbol} after {max_retries} attempts")
                    failed_stocks.append(symbol)
        
        if not hist_data:
            continue
        
        # Insert historical data
        try:
            inserted_count = db.insert_history_data(stock_id, hist_data)
            
            if inserted_count > 0:
                logging.info(f"Inserted {inserted_count} new data points for {symbol}")
                success_count += 1
            else:
                logging.info(f"No new data points for {symbol} (all dates already exist in database)")
                skipped_count += 1
        except Exception as e:
            logging.error(f"Database error while inserting data for {symbol}: {e}")
            failed_stocks.append(symbol)
        
        # Sleep to avoid hitting rate limits
        time.sleep(1)
    
    logging.info(f"Successfully updated data for {success_count} stocks")
    logging.info(f"Skipped {skipped_count} stocks (no new data)")
    if failed_stocks:
        logging.warning(f"Failed to update data for {len(failed_stocks)} stocks: {', '.join(failed_stocks[:10])}")
        if len(failed_stocks) > 10:
            logging.warning(f"... and {len(failed_stocks) - 10} more")
    db.close()

def run_scheduler():
    schedule.every().day.at("21:10").do(update_latest_stock_data)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    start_time = datetime.now()
    logging.info(f"Starting daily data update at {start_time}")
    
    run_scheduler()
    
    end_time = datetime.now()
    logging.info(f"Completed in {end_time - start_time}")