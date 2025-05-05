#!/usr/bin/env python
"""
AI Signal Generator module for stock trading signals.
This module processes stock data and generates signals using AI models.
"""

import logging
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AISignalGenerator:
    def __init__(self, db_path='stock_data.db'):
        """Initialize the AI Signal Generator."""
        self.db_path = db_path
        self.conn = None
        self.connect_db()
        
        # Create signals table if it doesn't exist
        self.create_signals_table()
        
    def connect_db(self):
        """Connect to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            return False
            
    def create_signals_table(self):
        """Create the signals table if it doesn't exist."""
        if not self.conn:
            if not self.connect_db():
                return False
                
        try:
            cursor = self.conn.cursor()
            
            # Create the signals table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stock_id INTEGER,
                    symbol TEXT,
                    signal_date TEXT,
                    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ai_signal TEXT,
                    confidence REAL,
                    ai_score REAL,
                    close REAL,
                    rsi REAL,
                    sma20 REAL,
                    sma50 REAL,
                    combined_signal TEXT,
                    combined_signal_desc TEXT,
                    notes TEXT,
                    FOREIGN KEY(stock_id) REFERENCES stocks(id)
                )
            """)
            
            # Create index for faster lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_signals_date 
                ON stock_signals(symbol, signal_date)
            """)
            
            self.conn.commit()
            logging.info("Signals table created or already exists")
            return True
            
        except sqlite3.Error as e:
            logging.error(f"Database error creating signals table: {e}")
            return False
            
    def close_db(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")
    
    def get_historical_data(self, symbol, days=100):
        """
        Get historical data for a specific stock symbol.
        
        Args:
            symbol (str): The stock symbol
            days (int): Number of days of historical data to retrieve
            
        Returns:
            pd.DataFrame: DataFrame containing historical data
        """
        if not self.conn:
            if not self.connect_db():
                return None
                
        try:
            # Calculate the date for 'days' ago
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            start_date_str = start_date.strftime("%Y-%m-%d")
            
            # SQL query to get historical data
            query = """
                SELECT h.date, h.open, h.high, h.low, h.close, h.volume
                FROM history_data h
                JOIN stocks s ON h.stock_id = s.id
                WHERE s.symbol = ?
                AND h.date >= ?
                ORDER BY h.date ASC
            """
            
            # Execute query
            df = pd.read_sql_query(query, self.conn, params=(symbol, start_date_str))
            
            if len(df) == 0:
                logging.warning(f"No historical data found for {symbol}")
                return None
                
            # Convert date to datetime
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            return df
            
        except sqlite3.Error as e:
            logging.error(f"Database error getting historical data for {symbol}: {e}")
            return None
    
    def generate_signals(self, symbol):
        """
        Generate AI-enhanced signals for a specific stock.
        
        Args:
            symbol (str): The stock symbol
            
        Returns:
            dict: Dictionary containing signals and scores
        """
        # Get historical data
        df = self.get_historical_data(symbol)
        if df is None or len(df) < 30:
            logging.warning(f"Insufficient data for {symbol}, need at least 30 data points")
            return None
            
        # Calculate basic indicators (for example purposes)
        # In a real implementation, this would use AI models
        try:
            # Calculate moving averages
            df['SMA20'] = df['close'].rolling(window=20).mean()
            df['SMA50'] = df['close'].rolling(window=50).mean()
            
            # RSI calculation
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # Basic signals (simulating AI-generated signals)
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            
            # Some basic signal logic
            trend_signal = 1 if latest['SMA20'] > latest['SMA50'] else -1
            momentum_signal = 1 if latest['RSI'] > prev['RSI'] else -1
            overbought = 1 if latest['RSI'] < 70 else -1
            oversold = 1 if latest['RSI'] > 30 else 1
            
            # Combine signals (in a real AI system, this would be an ML prediction)
            ai_score = (trend_signal + momentum_signal + overbought + oversold) / 4
            
            # Convert to simplified signal
            if ai_score > 0.5:
                signal = "BUY"
                confidence = ai_score
            elif ai_score < -0.5:
                signal = "SELL"
                confidence = -ai_score
            else:
                signal = "HOLD"
                confidence = 1 - abs(ai_score)
                
            return {
                "symbol": symbol,
                "date": df.index[-1].strftime("%Y-%m-%d"),
                "ai_signal": signal,
                "confidence": round(confidence * 100, 2),
                "ai_score": round(ai_score, 2),
                "close": latest['close'],
                "rsi": round(latest['RSI'], 2),
                "sma20": round(latest['SMA20'], 2),
                "sma50": round(latest['SMA50'], 2)
            }
            
        except Exception as e:
            logging.error(f"Error generating AI signals for {symbol}: {e}")
            return None
    
    def save_signal_to_db(self, signal_data):
        """
        Save a signal to the database.
        
        Args:
            signal_data (dict): Signal data to save
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.conn:
            if not self.connect_db():
                return False
                
        try:
            cursor = self.conn.cursor()
            
            # Get stock_id for the symbol
            cursor.execute("SELECT id FROM stocks WHERE symbol = ?", (signal_data.get('symbol'),))
            result = cursor.fetchone()
            
            if not result:
                logging.warning(f"Stock not found for symbol: {signal_data.get('symbol')}")
                return False
                
            stock_id = result[0]
            
            # Check if this signal already exists for this date
            cursor.execute(
                "SELECT id FROM stock_signals WHERE symbol = ? AND signal_date = ?", 
                (signal_data.get('symbol'), signal_data.get('date'))
            )
            
            existing_signal = cursor.fetchone()
            
            # Prepare query parameters
            params = (
                stock_id,
                signal_data.get('symbol'),
                signal_data.get('date'),
                signal_data.get('ai_signal'),
                signal_data.get('confidence'),
                signal_data.get('ai_score'),
                signal_data.get('close'),
                signal_data.get('rsi'),
                signal_data.get('sma20'),
                signal_data.get('sma50'),
                signal_data.get('combined_signal'),
                signal_data.get('combined_signal_desc'),
                signal_data.get('notes')
            )
            
            if existing_signal:
                # Update existing signal
                cursor.execute("""
                    UPDATE stock_signals SET 
                    stock_id = ?, 
                    symbol = ?,
                    signal_date = ?,
                    generated_at = CURRENT_TIMESTAMP,
                    ai_signal = ?,
                    confidence = ?,
                    ai_score = ?,
                    close = ?,
                    rsi = ?,
                    sma20 = ?,
                    sma50 = ?,
                    combined_signal = ?,
                    combined_signal_desc = ?,
                    notes = ?
                    WHERE id = ?
                """, params + (existing_signal[0],))
                
                logging.info(f"Updated signal for {signal_data.get('symbol')} on {signal_data.get('date')}")
            else:
                # Insert new signal
                cursor.execute("""
                    INSERT INTO stock_signals (
                        stock_id, symbol, signal_date, ai_signal, confidence, ai_score,
                        close, rsi, sma20, sma50, combined_signal, combined_signal_desc, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, params)
                
                logging.info(f"Inserted new signal for {signal_data.get('symbol')} on {signal_data.get('date')}")
            
            self.conn.commit()
            return True
            
        except sqlite3.Error as e:
            logging.error(f"Database error saving signal: {e}")
            return False
    
    def analyze_multiple_stocks(self, symbols=None, top_n=None):
        """
        Generate AI signals for multiple stocks.
        
        Args:
            symbols (list): List of stock symbols to analyze (optional)
            top_n (int): Number of top stocks to return, sorted by confidence (optional)
            
        Returns:
            list: List of dictionaries containing signals for each stock
        """
        if not self.conn:
            if not self.connect_db():
                return []
                
        try:
            # Get all stocks from database if symbols not provided
            if not symbols:
                cursor = self.conn.cursor()
                cursor.execute("SELECT symbol FROM stocks")
                symbols = [row[0] for row in cursor.fetchall()]
                
            results = []
            for symbol in symbols:
                signals = self.generate_signals(symbol)
                if signals:
                    results.append(signals)
                    
            # Sort by confidence and limit if top_n specified
            if top_n and len(results) > top_n:
                results = sorted(results, key=lambda x: x['confidence'], reverse=True)[:top_n]
                
            return results
            
        except Exception as e:
            logging.error(f"Error analyzing multiple stocks: {e}")
            return []
        finally:
            # Don't close the db here, let the caller close it

# For testing
if __name__ == "__main__":
    generator = AISignalGenerator()
    test_symbol = "SBIN"  # Example symbol
    signals = generator.generate_signals(test_symbol)
    print(f"AI Signals for {test_symbol}: {signals}")
    generator.close_db()