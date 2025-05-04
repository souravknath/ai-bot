import sqlite3
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseHandler:
    def __init__(self, db_name='stock_data.db'):
        """Initialize the database connection"""
        self.db_name = db_name
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the database"""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            logging.info(f"Connected to database: {self.db_name}")
            self.create_tables()
            return True
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            return False
            
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")
            
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            # Create stocks table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS stocks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    security_id TEXT UNIQUE,
                    exchange_segment TEXT,
                    symbol TEXT,
                    name TEXT,
                    instrument TEXT,
                    added_date TEXT,
                    last_updated TEXT
                )
            ''')
            
            # Create history_data table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS history_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_id INTEGER,
                    timestamp INTEGER,
                    date TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    open_interest INTEGER,
                    security_id TEXT,
                    FOREIGN KEY (stock_id) REFERENCES stocks (id),
                    UNIQUE (stock_id, timestamp)
                )
            ''')
            
            # Create settings table for auto order configuration
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE,
                    value TEXT,
                    value_type TEXT,
                    description TEXT,
                    last_updated TEXT
                )
            ''')
            
            # Create watchlist table for auto order enabled symbols
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS watchlist (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_id INTEGER,
                    symbol TEXT,
                    added_date TEXT,
                    FOREIGN KEY (stock_id) REFERENCES stocks (id),
                    UNIQUE (stock_id)
                )
            ''')
            
            self.conn.commit()
            logging.info("Tables created successfully")
            return True
        except sqlite3.Error as e:
            logging.error(f"Error creating tables: {e}")
            self.conn.rollback()
            return False
    
    def insert_stock(self, security_id, exchange_segment, symbol, name, instrument):
        """Insert a new stock or update if it exists"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO stocks 
                (security_id, exchange_segment, symbol, name, instrument, 
                added_date, last_updated)
                VALUES (?, ?, ?, ?, ?, 
                COALESCE((SELECT added_date FROM stocks WHERE security_id = ?), ?), ?)
            ''', (security_id, exchange_segment, symbol, name, instrument, 
                 security_id, current_time, current_time))
            self.conn.commit()
            
            # Get the ID of the inserted/updated stock
            self.cursor.execute("SELECT id FROM stocks WHERE security_id = ?", (security_id,))
            stock_id = self.cursor.fetchone()[0]
            return stock_id
        except sqlite3.Error as e:
            logging.error(f"Error inserting stock: {e}")
            self.conn.rollback()
            return None
    
    def insert_history_data(self, stock_id, data):
        """Insert historical data for a stock"""
        try:
            count = 0
            
            # Get the security_id for this stock_id
            self.cursor.execute("SELECT security_id FROM stocks WHERE id = ?", (stock_id,))
            result = self.cursor.fetchone()
            security_id = result[0] if result else None
            
            # Check if data contains 'candles' (API response format)
            if data and 'candles' in data:
                candles = data['candles']
                
                for candle in candles:
                    if len(candle) >= 6:  # Make sure we have enough data points
                        # Candle format: [date, open, high, low, close, volume]
                        date_str = candle[0]
                        try:
                            # Try parsing the date string to get a timestamp
                            timestamp = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())
                        except ValueError:
                            # If date parsing fails, use current timestamp as fallback
                            timestamp = int(datetime.now().timestamp())
                            
                        open_price = candle[1] if len(candle) > 1 else 0
                        high_price = candle[2] if len(candle) > 2 else 0
                        low_price = candle[3] if len(candle) > 3 else 0
                        close_price = candle[4] if len(candle) > 4 else 0
                        volume = candle[5] if len(candle) > 5 else 0
                        open_interest = candle[6] if len(candle) > 6 else 0
                        
                        # Insert data
                        self.cursor.execute('''
                            INSERT OR REPLACE INTO history_data 
                            (stock_id, timestamp, date, open, high, low, close, volume, open_interest, security_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (stock_id, timestamp, date_str, open_price, high_price, low_price, 
                              close_price, volume, open_interest, security_id))
                        count += 1
            # Handle legacy format (with separate arrays)
            elif data and 'timestamp' in data:
                for i in range(len(data['timestamp'])):
                    timestamp = data['timestamp'][i]
                    date_str = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                    
                    # Get data for this timestamp
                    open_price = data['open'][i] if 'open' in data and i < len(data['open']) else 0
                    high_price = data['high'][i] if 'high' in data and i < len(data['high']) else 0
                    low_price = data['low'][i] if 'low' in data and i < len(data['low']) else 0
                    close_price = data['close'][i] if 'close' in data and i < len(data['close']) else 0
                    volume = data['volume'][i] if 'volume' in data and i < len(data['volume']) else 0
                    open_interest = data['open_interest'][i] if 'open_interest' in data and i < len(data['open_interest']) else 0
                    
                    # Insert data
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO history_data 
                        (stock_id, timestamp, date, open, high, low, close, volume, open_interest, security_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (stock_id, timestamp, date_str, open_price, high_price, low_price, 
                          close_price, volume, open_interest, security_id))
                    count += 1
            else:
                logging.error("Invalid data format for historical data")
                return 0
                
            self.conn.commit()
            return count
        except sqlite3.Error as e:
            logging.error(f"Error inserting history data: {e}")
            self.conn.rollback()
            return 0
    
    def get_all_stocks(self):
        """Get all stocks from the database"""
        try:
            self.cursor.execute("SELECT id, security_id, exchange_segment, symbol, instrument FROM stocks")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logging.error(f"Error retrieving stocks: {e}")
            return []
    
    def get_stock_by_id(self, stock_id):
        """Get a stock by its ID"""
        try:
            self.cursor.execute(
                "SELECT id, security_id, exchange_segment, symbol, instrument FROM stocks WHERE id = ?", 
                (stock_id,)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logging.error(f"Error retrieving stock: {e}")
            return None
            
    def clean_history_data(self, older_than_days=None, stock_id=None, before_date=None, all_data=False):
        """Clean history data based on specified criteria"""
        try:
            conditions = []
            params = []
            
            if all_data:
                # Delete all data
                query = "DELETE FROM history_data"
            else:
                # Build query based on filters
                if older_than_days:
                    cutoff_timestamp = int((datetime.now().timestamp()) - (older_than_days * 86400))
                    conditions.append("timestamp < ?")
                    params.append(cutoff_timestamp)
                
                if stock_id:
                    conditions.append("stock_id = ?")
                    params.append(stock_id)
                
                if before_date:
                    try:
                        date_timestamp = int(datetime.strptime(before_date, "%Y-%m-%d").timestamp())
                        conditions.append("timestamp < ?")
                        params.append(date_timestamp)
                    except ValueError:
                        logging.error(f"Invalid date format: {before_date}. Expected YYYY-MM-DD")
                        return 0
                
                if not conditions:
                    logging.error("No valid conditions provided for cleaning")
                    return 0
                
                query = "DELETE FROM history_data WHERE " + " AND ".join(conditions)
            
            # Execute the delete query
            self.cursor.execute(query, params)
            deleted_count = self.cursor.rowcount
            self.conn.commit()
            
            logging.info(f"Deleted {deleted_count} records from history_data")
            return deleted_count
            
        except sqlite3.Error as e:
            logging.error(f"Error cleaning history data: {e}")
            self.conn.rollback()
            return 0
    
    def update_security_ids(self):
        """Update security_id column in history_data table for existing records"""
        try:
            # Find records with empty or NULL security_id
            self.cursor.execute("""
                UPDATE history_data 
                SET security_id = (
                    SELECT security_id 
                    FROM stocks 
                    WHERE stocks.id = history_data.stock_id
                )
                WHERE history_data.security_id IS NULL 
                OR history_data.security_id = ''
            """)
            
            updated_count = self.cursor.rowcount
            self.conn.commit()
            logging.info(f"Updated security_id for {updated_count} records in history_data")
            return updated_count
        except sqlite3.Error as e:
            logging.error(f"Error updating security_ids: {e}")
            self.conn.rollback()
            return 0

    def data_exists_for_security_and_date(self, security_id, date_str):
        """Check if historical data exists for a specific security ID and date"""
        try:
            query = """
                SELECT 1 FROM history_data h
                JOIN stocks s ON h.stock_id = s.id
                WHERE s.security_id = ? AND h.date = ?
                LIMIT 1
            """
            self.cursor.execute(query, (security_id, date_str))
            result = self.cursor.fetchone()
            return result is not None
        except sqlite3.Error as e:
            logging.error(f"Error checking data existence: {e}")
            return False

    def get_setting(self, key, default=None):
        """Get a setting value from the database by key"""
        try:
            self.cursor.execute(
                "SELECT value, value_type FROM settings WHERE key = ?", 
                (key,)
            )
            result = self.cursor.fetchone()
            
            if result:
                value, value_type = result
                # Convert value based on its type
                if value_type == 'int':
                    return int(value)
                elif value_type == 'float':
                    return float(value)
                elif value_type == 'bool':
                    return value.lower() == 'true'
                else:  # Treat as string by default
                    return value
            return default
        except sqlite3.Error as e:
            logging.error(f"Error retrieving setting {key}: {e}")
            return default
            
    def set_setting(self, key, value, description=None):
        """Save a setting value to the database"""
        try:
            # Determine the value type
            if isinstance(value, bool):
                value_str = str(value).lower()
                value_type = 'bool'
            elif isinstance(value, int):
                value_str = str(value)
                value_type = 'int'
            elif isinstance(value, float):
                value_str = str(value)
                value_type = 'float'
            else:
                value_str = str(value)
                value_type = 'string'
                
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Check if description is provided
            if description is None:
                # Get existing description if any
                self.cursor.execute("SELECT description FROM settings WHERE key = ?", (key,))
                result = self.cursor.fetchone()
                if result:
                    description = result[0]
                else:
                    description = ""
            
            self.cursor.execute('''
                INSERT OR REPLACE INTO settings 
                (key, value, value_type, description, last_updated)
                VALUES (?, ?, ?, ?, ?)
            ''', (key, value_str, value_type, description, current_time))
            
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logging.error(f"Error saving setting {key}: {e}")
            self.conn.rollback()
            return False
            
    def get_all_settings(self):
        """Get all settings as a dictionary"""
        try:
            self.cursor.execute("SELECT key, value, value_type FROM settings")
            results = self.cursor.fetchall()
            
            settings = {}
            for key, value, value_type in results:
                # Convert value based on its type
                if value_type == 'int':
                    settings[key] = int(value)
                elif value_type == 'float':
                    settings[key] = float(value)
                elif value_type == 'bool':
                    settings[key] = value.lower() == 'true'
                else:  # Treat as string by default
                    settings[key] = value
                    
            return settings
        except sqlite3.Error as e:
            logging.error(f"Error retrieving all settings: {e}")
            return {}
            
    def add_to_watchlist(self, symbol):
        """Add a symbol to the watchlist"""
        try:
            # First, find the stock ID for this symbol
            self.cursor.execute("SELECT id FROM stocks WHERE symbol = ?", (symbol,))
            result = self.cursor.fetchone()
            
            if not result:
                logging.error(f"Symbol {symbol} not found in stocks table")
                return False
                
            stock_id = result[0]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Insert into watchlist
            self.cursor.execute('''
                INSERT OR REPLACE INTO watchlist
                (stock_id, symbol, added_date) 
                VALUES (?, ?, ?)
            ''', (stock_id, symbol, current_time))
            
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logging.error(f"Error adding {symbol} to watchlist: {e}")
            self.conn.rollback()
            return False
            
    def remove_from_watchlist(self, symbol):
        """Remove a symbol from the watchlist"""
        try:
            self.cursor.execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
            self.conn.commit()
            return self.cursor.rowcount > 0
        except sqlite3.Error as e:
            logging.error(f"Error removing {symbol} from watchlist: {e}")
            self.conn.rollback()
            return False
            
    def clear_watchlist(self):
        """Clear all symbols from the watchlist"""
        try:
            self.cursor.execute("DELETE FROM watchlist")
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logging.error(f"Error clearing watchlist: {e}")
            self.conn.rollback()
            return False
            
    def get_watchlist(self):
        """Get all symbols in the watchlist"""
        try:
            self.cursor.execute("SELECT symbol FROM watchlist")
            results = self.cursor.fetchall()
            return [row[0] for row in results]
        except sqlite3.Error as e:
            logging.error(f"Error getting watchlist: {e}")
            return []
