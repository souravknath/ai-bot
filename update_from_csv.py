import sqlite3
import csv
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"security_update_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)

class SecurityIDUpdater:
    def __init__(self, csv_path):
        """Initialize the SecurityIDUpdater with database and csv file details"""
        self.db_name = 'stock_data.db'
        self.csv_path = csv_path
        self.conn = None
        self.cursor = None
    
    def connect_to_db(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to database: {self.db_name}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            return False
    
    def close_db(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")
    
    def read_csv_data(self):
        """Read security ID data from CSV file"""
        try:
            if not os.path.exists(self.csv_path):
                logging.error(f"CSV file not found: {self.csv_path}")
                return None
            
            stocks = []
            with open(self.csv_path, 'r') as csv_file:
                reader = csv.reader(csv_file)
                for row in reader:
                    if len(row) >= 6:
                        # CSV format: NSE,E,11373,INE376G01013,EQUITY,,BIOCON...
                        security_id = row[2]
                        exchange_segment = "NSE_EQ"
                        symbol = row[6]
                        name = row[7] if len(row) > 7 else symbol
                        instrument = "EQUITY"
                        
                        if symbol and security_id:
                            stock_data = {
                                "security_id": security_id,
                                "exchange_segment": exchange_segment,
                                "symbol": symbol,
                                "name": name,
                                "instrument": instrument
                            }
                            stocks.append(stock_data)
            
            logging.info(f"Read {len(stocks)} stocks from CSV file")
            return stocks
        
        except Exception as e:
            logging.error(f"Error reading CSV file: {e}")
            return None
    
    def update_security_ids(self, stocks):
        """Update security IDs in the database"""
        if not stocks:
            return 0
        
        updated_count = 0
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for stock in stocks:
            try:
                # First check if stock exists
                self.cursor.execute(
                    "SELECT id FROM stocks WHERE symbol = ?",
                    (stock["symbol"],)
                )
                result = self.cursor.fetchone()
                
                if result:
                    # Update existing stock
                    self.cursor.execute('''
                        UPDATE stocks 
                        SET security_id = ?, exchange_segment = ?, name = ?, instrument = ?, last_updated = ?
                        WHERE symbol = ?
                    ''', (
                        stock["security_id"], 
                        stock["exchange_segment"], 
                        stock["name"], 
                        stock["instrument"], 
                        current_time, 
                        stock["symbol"]
                    ))
                    
                    if self.cursor.rowcount > 0:
                        updated_count += 1
                        logging.info(f"Updated security ID for {stock['symbol']} to {stock['security_id']}")
                else:
                    # Insert new stock
                    self.cursor.execute('''
                        INSERT INTO stocks 
                        (security_id, exchange_segment, symbol, name, instrument, added_date, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        stock["security_id"], 
                        stock["exchange_segment"], 
                        stock["symbol"], 
                        stock["name"], 
                        stock["instrument"], 
                        current_time, 
                        current_time
                    ))
                    
                    updated_count += 1
                    logging.info(f"Added new stock {stock['symbol']} with security_id {stock['security_id']}")
            
            except sqlite3.Error as e:
                logging.error(f"Error updating security ID for {stock['symbol']}: {e}")
        
        self.conn.commit()
        return updated_count

def main():
    """Main function to update security IDs from CSV"""
    csv_path = "c:\\Users\\user\\Downloads\\api-scrip-master-detailed.csv"
    
    updater = SecurityIDUpdater(csv_path)
    
    # Connect to database
    if not updater.connect_to_db():
        return
    
    # Read data from CSV
    stocks = updater.read_csv_data()
    if not stocks:
        updater.close_db()
        return
    
    # Update security IDs
    updated_count = updater.update_security_ids(stocks)
    print(f"Updated {updated_count} security IDs in the database.")
    
    updater.close_db()
    print("Security ID update complete.")
    print("You can now run update_daily_data.py to fetch historical data.")

if __name__ == "__main__":
    main()