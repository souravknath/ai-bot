#!/usr/bin/env python
import pandas as pd
import numpy as np
import sqlite3
import logging
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from plotly.offline import plot
import webbrowser
from datetime import datetime, timedelta
import argparse

# Import AI signal components
try:
    from ai_enhanced_signals import AIEnhancedSignalGenerator
    AI_AVAILABLE = True
    logging.info("AI signal enhancement modules loaded successfully")
except ImportError as e:
    AI_AVAILABLE = False
    logging.warning(f"AI signal enhancement modules not available: {str(e)}")

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f"signals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

class SignalGenerator:
    def __init__(self, db_path='stock_data.db'):
        """Initialize the signal generator with database connection"""
        self.db_path = db_path
        self.conn = None
        self.use_ai = AI_AVAILABLE
        
        # Initialize AI components if available
        if self.use_ai:
            try:
                self.ai_signals = AIEnhancedSignalGenerator()
                logging.info("AI signal generator initialized")
            except Exception as e:
                self.use_ai = False
                logging.error(f"Failed to initialize AI components: {str(e)}")
        
    def connect_db(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logging.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logging.error(f"Database connection error: {e}")
            return False
            
    def close_db(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")
            
    def get_stock_data(self, symbol=None, security_id=None, days=100):
        """
        Fetch historical stock data for the specified symbol or security_id
        
        Args:
            symbol: Stock symbol (e.g., 'HDFCBANK')
            security_id: Security ID (e.g., 'INE040A01034')
            days: Number of days of historical data to fetch
        
        Returns:
            DataFrame with historical price data
        """
        if not self.conn:
            if not self.connect_db():
                return None
                
        try:
            # Calculate start date
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            from_date = start_date.strftime("%Y-%m-%d")
            to_date = end_date.strftime("%Y-%m-%d")
            
            # Build query parameters
            query_params = []
            conditions = []
            
            if symbol:
                conditions.append("s.symbol = ?")
                query_params.append(symbol)
            
            if security_id:
                conditions.append("h.security_id = ?")
                query_params.append(security_id)
                
            if not conditions:
                logging.error("Either symbol or security_id must be provided")
                return None
                
            # Add date range
            query_params.extend([from_date, to_date])
            
            # Build query
            query = f"""
                SELECT h.timestamp, h.date, h.open, h.high, h.low, h.close, h.volume,
                       s.symbol, s.name, s.security_id
                FROM history_data h
                JOIN stocks s ON h.stock_id = s.id
                WHERE {" AND ".join(conditions)}
                AND h.date BETWEEN ? AND ?
                ORDER BY h.timestamp
            """
            
            # Execute query
            df = pd.read_sql_query(query, self.conn, params=tuple(query_params))
            
            if len(df) == 0:
                logging.warning(f"No data found for the specified stock and date range")
                return None
            
            logging.info(f"Retrieved {len(df)} data points for {symbol or security_id}")
            return df
            
        except sqlite3.Error as e:
            logging.error(f"Error fetching stock data: {e}")
            return None
            
    def calculate_sma(self, df, period=50):
        """Calculate Simple Moving Average"""
        if df is None or len(df) < period:
            logging.warning(f"Insufficient data to calculate {period}-day SMA")
            return None
            
        df = df.copy()
        df[f'SMA_{period}'] = df['close'].rolling(window=period).mean()
        return df
        
    def calculate_rsi(self, df, period=14):
        """Calculate Relative Strength Index (RSI)"""
        if df is None or len(df) < period + 1:
            logging.warning(f"Insufficient data to calculate RSI with period {period}")
            return None
            
        df = df.copy()
        # Calculate price changes
        delta = df['close'].diff()
        
        # Separate gains (up) and losses (down)
        gain = delta.copy()
        loss = delta.copy()
        gain[gain < 0] = 0
        loss[loss > 0] = 0
        loss = abs(loss)
        
        # Calculate average gain and average loss
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # Calculate RS and RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        df['RSI'] = rsi
        return df
        
    def generate_signals(self, df, sma_period=50, rsi_period=14, rsi_threshold=50):
        """Generate trading signals based on SMA crossover and RSI threshold"""
        if df is None:
            return None
            
        # Calculate indicators
        df = self.calculate_sma(df, sma_period)
        df = self.calculate_rsi(df, rsi_period)
        
        if df is None:
            return None
            
        # Create signal columns
        df['MA_Signal'] = 0
        df['RSI_Signal'] = 0
        
        # Generate SMA crossover signals (1: bullish, -1: bearish)
        df['Price_Above_MA'] = (df['close'] > df[f'SMA_{sma_period}']).astype(int)
        df['MA_Signal'] = df['Price_Above_MA'].diff()
        
        # Generate RSI signals (1: bullish when RSI crosses above 50, -1: bearish when RSI crosses below 50)
        df['RSI_Above_50'] = (df['RSI'] > rsi_threshold).astype(int)
        df['RSI_Signal'] = df['RSI_Above_50'].diff()
        
        # Combined signal (both indicators agree) - Only buy signals when both criteria met
        df['Combined_Signal'] = 0
        buy_condition = (df['MA_Signal'] > 0) & (df['RSI_Signal'] > 0)
        
        df.loc[buy_condition, 'Combined_Signal'] = 1  # Buy signal only when both conditions met
        
        return df
        
    def create_signal_chart(self, df, symbol, output_dir="signal_charts"):
        """Create an interactive chart showing price, SMA, RSI, and signals"""
        if df is None or len(df) < 2:
            logging.error("Insufficient data for charting")
            return None
            
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Convert date to datetime if it's not already
        if not pd.api.types.is_datetime64_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
            
        # Create subplots: 2 rows, 1 column (price chart and RSI indicator)
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                            vertical_spacing=0.1, 
                            row_heights=[0.7, 0.3],
                            subplot_titles=(f"{symbol} Price and 50-day MA", "RSI (14)"))
        
        # Add price candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=df['date'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name=symbol,
                showlegend=False
            ),
            row=1, col=1
        )
        
        # Add 50-day SMA
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['SMA_50'],
                line=dict(color='blue', width=1),
                name='50-day MA'
            ),
            row=1, col=1
        )
        
        # Add RSI
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['RSI'],
                line=dict(color='purple', width=1),
                name='RSI (14)'
            ),
            row=2, col=1
        )
        
        # Add horizontal line at RSI=50
        fig.add_hline(y=50, line_dash="dash", line_color="gray", row=2, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
        
        # Add buy signals (MA crossover)
        buy_signals_ma = df[df['MA_Signal'] > 0]
        if not buy_signals_ma.empty:
            fig.add_trace(
                go.Scatter(
                    x=buy_signals_ma['date'],
                    y=buy_signals_ma['low'] * 0.99,  # Slightly below the price
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=10, color='green'),
                    name='MA Buy Signal'
                ),
                row=1, col=1
            )
        
        # Add sell signals (MA crossover)
        sell_signals_ma = df[df['MA_Signal'] < 0]
        if not sell_signals_ma.empty:
            fig.add_trace(
                go.Scatter(
                    x=sell_signals_ma['date'],
                    y=sell_signals_ma['high'] * 1.01,  # Slightly above the price
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=10, color='red'),
                    name='MA Sell Signal'
                ),
                row=1, col=1
            )
            
        # Add RSI buy signals (crossing above 50)
        rsi_buy = df[df['RSI_Signal'] > 0]
        if not rsi_buy.empty:
            fig.add_trace(
                go.Scatter(
                    x=rsi_buy['date'],
                    y=[45] * len(rsi_buy),
                    mode='markers',
                    marker=dict(symbol='triangle-up', size=8, color='green'),
                    name='RSI Buy Signal'
                ),
                row=2, col=1
            )
        
        # Add RSI sell signals (crossing below 50)
        rsi_sell = df[df['RSI_Signal'] < 0]
        if not rsi_sell.empty:
            fig.add_trace(
                go.Scatter(
                    x=rsi_sell['date'],
                    y=[55] * len(rsi_sell),
                    mode='markers',
                    marker=dict(symbol='triangle-down', size=8, color='red'),
                    name='RSI Sell Signal'
                ),
                row=2, col=1
            )
        
        # Add combined strong signals (both indicators agree)
        strong_buy = df[df['Combined_Signal'] > 0]
        if not strong_buy.empty:
            fig.add_trace(
                go.Scatter(
                    x=strong_buy['date'],
                    y=strong_buy['low'] * 0.98,
                    mode='markers+text',
                    marker=dict(symbol='star', size=16, color='lime'),
                    text=['BUY'] * len(strong_buy),
                    textposition="bottom center",
                    name='Strong Buy'
                ),
                row=1, col=1
            )
            
        strong_sell = df[df['Combined_Signal'] < 0]
        if not strong_sell.empty:
            fig.add_trace(
                go.Scatter(
                    x=strong_sell['date'],
                    y=strong_sell['high'] * 1.02,
                    mode='markers+text',
                    marker=dict(symbol='star', size=16, color='darkred'),
                    text=['SELL'] * len(strong_sell),
                    textposition="top center",
                    name='Strong Sell'
                ),
                row=1, col=1
            )
            
        # Update layout
        fig.update_layout(
            title=f"{symbol} - Technical Signals (50-day MA Crossover and RSI 50)",
            xaxis_title="Date",
            yaxis_title="Price",
            height=800,
            xaxis_rangeslider_visible=False,
            template="plotly_white"
        )
        
        # Date for the filename
        current_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"{output_dir}/{symbol}_signals_{current_date}.html"
        
        # Save the chart
        plot(fig, filename=filename, auto_open=False)
        logging.info(f"Chart saved to {filename}")
        
        return filename
    
    def get_latest_signals(self, df):
        """Extract the latest signals from the dataframe"""
        if df is None or len(df) < 2:
            return None
            
        # Get the most recent data point
        latest = df.iloc[-1]
        previous = df.iloc[-2]
        
        # Look for recent crossovers (within last 1-2 candles)
        recent_ma_crossover = False
        recent_rsi_crossover = False
        
        # Check if signal occurred in the most recent candle
        if latest['MA_Signal'] != 0:
            recent_ma_crossover = True
        # Check if signal occurred in the second-to-last candle
        elif len(df) >= 3 and df.iloc[-2]['MA_Signal'] != 0:
            recent_ma_crossover = True
            
        # Check for recent RSI crossover
        if latest['RSI_Signal'] != 0:
            recent_rsi_crossover = True
        elif len(df) >= 3 and df.iloc[-2]['RSI_Signal'] != 0:
            recent_rsi_crossover = True
        
        # Calculate change percent from previous close
        if 'close' in previous and previous['close'] > 0:
            change_percent = ((latest['close'] - previous['close']) / previous['close']) * 100
        else:
            change_percent = 0
            
        # Get signal data
        signals = {
            'date': latest['date'],
            'symbol': latest['symbol'] if 'symbol' in latest else None,
            'name': latest.get('name', 'Unknown'),
            'security_id': latest['security_id'] if 'security_id' in latest else None,
            'close': latest['close'],
            'ma_50': latest['SMA_50'],
            'rsi': latest['RSI'],
            'price_vs_ma': 'ABOVE' if latest['close'] > latest['SMA_50'] else 'BELOW',
            'ma_signal': latest['MA_Signal'],
            'rsi_signal': latest['RSI_Signal'],
            'combined_signal': latest['Combined_Signal'],
            'ma_signal_desc': 'NEUTRAL',
            'rsi_signal_desc': 'NEUTRAL',
            'combined_signal_desc': 'NEUTRAL',
            'recent_ma_crossover': recent_ma_crossover,
            'recent_rsi_crossover': recent_rsi_crossover,
            'change_percent': change_percent,
            'volume': latest.get('volume', 0)
        }
        
        # Translate signals to text descriptions
        if latest['MA_Signal'] > 0:
            signals['ma_signal_desc'] = 'BUY'
        elif latest['MA_Signal'] < 0:
            signals['ma_signal_desc'] = 'SELL'
            
        if latest['RSI_Signal'] > 0:
            signals['rsi_signal_desc'] = 'BUY'
        elif latest['RSI_Signal'] < 0:
            signals['rsi_signal_desc'] = 'SELL'
            
        if latest['Combined_Signal'] > 0:
            signals['combined_signal_desc'] = 'STRONG BUY'
        elif latest['Combined_Signal'] < 0:
            signals['combined_signal_desc'] = 'STRONG SELL'
        
        # Add indicator for very recent signals (happened within 1-2 candles)
        if recent_ma_crossover and recent_rsi_crossover:
            if signals['combined_signal_desc'] == 'STRONG BUY':
                signals['combined_signal_desc'] = 'FRESH STRONG BUY'
            elif signals['combined_signal_desc'] == 'STRONG SELL':
                signals['combined_signal_desc'] = 'FRESH STRONG SELL'
            
        return signals

    def get_latest_ai_enhanced_signals(self, df, sentiment_analysis=None):
        """Extract the latest signals including AI and sentiment data"""
        if df is None or len(df) < 2:
            return None
            
        # First get the traditional signals
        signals = self.get_latest_signals(df)
        if not signals:
            return None
            
        # Get the most recent data point
        latest = df.iloc[-1]
        
        # Add AI signal if available
        if 'AI_Signal_Desc' in latest:
            signals['ai_signal_desc'] = latest['AI_Signal_Desc']
            signals['ai_signal_prob'] = latest.get('AI_Signal_Prob', 0.5)
        elif 'ai_signal_desc' in latest:  # Alternative column name
            signals['ai_signal_desc'] = latest['ai_signal_desc']
            signals['ai_signal_prob'] = 0.5
        else:
            signals['ai_signal_desc'] = 'NEUTRAL'
            signals['ai_signal_prob'] = 0.5
            
        # Add sentiment data if available
        if sentiment_analysis:
            signals['sentiment_desc'] = sentiment_analysis['sentiment_desc']
            signals['sentiment_score'] = sentiment_analysis['sentiment_score']
            signals['sentiment_confidence'] = sentiment_analysis.get('confidence', 0)
        elif 'sentiment_desc' in latest and 'sentiment_score' in latest:
            signals['sentiment_desc'] = latest['sentiment_desc']
            signals['sentiment_score'] = latest['sentiment_score']
            signals['sentiment_confidence'] = 0.5
        else:
            signals['sentiment_desc'] = 'NEUTRAL'
            signals['sentiment_score'] = 0.5
            signals['sentiment_confidence'] = 0
            
        # Generate an AI-enhanced comprehensive signal
        self._add_ai_enhanced_signal(signals)
        
        return signals
        
    def _add_ai_enhanced_signal(self, signals):
        """Add a comprehensive AI-enhanced signal combining all signal sources"""
        # Weight for different signals
        traditional_weight = 0.5
        ai_weight = 0.3
        sentiment_weight = 0.2
        
        # Traditional signal score (-2 to 2)
        traditional_score = 0
        if signals['combined_signal_desc'] == 'STRONG BUY':
            traditional_score = 1
        elif signals['combined_signal_desc'] == 'FRESH STRONG BUY':
            traditional_score = 2
        elif signals['combined_signal_desc'] == 'STRONG SELL':
            traditional_score = -1
        elif signals['combined_signal_desc'] == 'FRESH STRONG SELL':
            traditional_score = -2
            
        # AI signal score (-2 to 2)
        ai_score = 0
        ai_signal = signals.get('ai_signal_desc', 'NEUTRAL')
        ai_prob = signals.get('ai_signal_prob', 0.5)
        
        if ai_signal == 'AI BUY':
            ai_score = 1 + (ai_prob - 0.7) * 3  # Scale from 1 to 2 based on confidence
        elif ai_signal == 'AI SELL':
            ai_score = -1 - (0.3 - ai_prob) * 3  # Scale from -1 to -2 based on confidence
            
        # Sentiment score (-2 to 2)
        sentiment_score = 0
        sentiment_desc = signals.get('sentiment_desc', 'NEUTRAL')
        sentiment_confidence = signals.get('sentiment_confidence', 0)
        
        if sentiment_desc == 'BULLISH':
            sentiment_score = 1 * sentiment_confidence  # Scale by confidence
        elif sentiment_desc == 'STRONGLY BULLISH':
            sentiment_score = 2 * sentiment_confidence
        elif sentiment_desc == 'BEARISH':
            sentiment_score = -1 * sentiment_confidence
        elif sentiment_desc == 'STRONGLY BEARISH':
            sentiment_score = -2 * sentiment_confidence
            
        # Calculate weighted final score
        final_score = (traditional_weight * traditional_score + 
                       ai_weight * ai_score + 
                       sentiment_weight * sentiment_score)
        
        # Determine final signal description based on score
        if final_score >= 1.2:
            signals['ai_enhanced_signal'] = 'STRONG AI BUY'
        elif final_score >= 0.6:
            signals['ai_enhanced_signal'] = 'AI BUY'
        elif final_score <= -1.2:
            signals['ai_enhanced_signal'] = 'STRONG AI SELL'
        elif final_score <= -0.6:
            signals['ai_enhanced_signal'] = 'AI SELL'
        else:
            signals['ai_enhanced_signal'] = 'NEUTRAL'
            
        # Add score for reference
        signals['ai_enhanced_score'] = round(final_score, 2)
        
        return signals
        
    def print_signals_summary(self, signals_list):
        """Print a summary of signals for multiple stocks"""
        if not signals_list:
            print("No signals available")
            return
            
        # Print header
        print("\n" + "="*100)
        print(f"{'Symbol':<10} {'Close':<10} {'SMA-50':<10} {'RSI':<10} {'Price vs MA':<12} {'MA Signal':<12} {'RSI Signal':<12} {'Combined':<12}")
        print("-"*100)
        
        # Print each stock's signals
        for signals in signals_list:
            if signals:
                print(f"{signals['symbol']:<10} {signals['close']:<10.2f} {signals['ma_50']:<10.2f} "
                      f"{signals['rsi']:<10.2f} {signals['price_vs_ma']:<12} {signals['ma_signal_desc']:<12} "
                      f"{signals['rsi_signal_desc']:<12} {signals['combined_signal_desc']:<12}")
        
        print("="*100)
        
    def analyze_stock(self, symbol=None, security_id=None, days=100, show_chart=True):
        """Analyze a stock and generate signals"""
        # Get historical data
        df = self.get_stock_data(symbol, security_id, days)
        if df is None:
            logging.error(f"Could not get data for {symbol or security_id}")
            return None

        # If AI is available, use it to optimize parameters
        optimal_params = {
            'ma_period': 50,
            'rsi_period': 14,
            'rsi_threshold': 50
        }
        
        if self.use_ai:
            try:
                # Get optimal parameters for this stock
                logging.info(f"Using AI to optimize parameters for {symbol}")
                optimal_params = self.ai_signals.optimize_parameters(df)
            except Exception as e:
                logging.error(f"Error optimizing parameters: {str(e)}")
                # Continue with default parameters on error
            
        # Generate signals with optimal parameters
        df_signals = self.generate_signals(
            df, 
            sma_period=optimal_params['ma_period'],
            rsi_period=optimal_params['rsi_period'],
            rsi_threshold=optimal_params['rsi_threshold']
        )
        
        if df_signals is None:
            logging.error(f"Could not generate signals for {symbol or security_id}")
            return None
        
        # If AI is available, enhance signals with AI predictions
        sentiment_analysis = None
        if self.use_ai:
            try:
                # Add AI predictions
                df_signals = self.ai_signals.enhance_with_ai(df_signals, symbol)
                
                # Add sentiment analysis if symbol is provided
                if symbol:
                    df_signals, sentiment_analysis = self.ai_signals.add_sentiment_analysis(df_signals, symbol)
            except Exception as e:
                logging.error(f"Error enhancing signals with AI: {str(e)}")
                # Continue without AI on error
        
        # Get latest signals (traditional or AI-enhanced)
        if self.use_ai:
            signals = self.get_latest_ai_enhanced_signals(df_signals, sentiment_analysis)
        else:
            signals = self.get_latest_signals(df_signals)
        
        # Save signals to database
        self.save_signals_to_db(signals)
        
        # Create chart if requested
        if show_chart and symbol:
            if self.use_ai:
                try:
                    # Create AI-enhanced chart
                    filename = self.ai_signals.create_ai_enhanced_chart(df_signals, symbol, sentiment_analysis)
                except Exception as e:
                    logging.error(f"Error creating AI-enhanced chart: {str(e)}. Falling back to traditional chart.")
                    filename = self.create_signal_chart(df_signals, symbol)
            else:
                filename = self.create_signal_chart(df_signals, symbol)
            
            # Open the chart file in a browser
            if filename and os.path.exists(filename):
                try:
                    webbrowser.open('file://' + os.path.abspath(filename))
                except Exception as e:
                    logging.error(f"Error opening browser: {e}")
        
        return signals
        
    def analyze_multiple_stocks(self, symbols=None, show_charts=False):
        """Analyze multiple stocks and generate signals for all of them"""
        if not symbols:
            # Get top stocks from database
            try:
                query = """
                    SELECT DISTINCT s.symbol, s.security_id
                    FROM stocks s
                    JOIN history_data h ON s.id = h.stock_id
                    GROUP BY s.id
                    ORDER BY COUNT(h.id) DESC
                    LIMIT 50
                """
                df_stocks = pd.read_sql_query(query, self.conn)
                symbols = df_stocks['symbol'].tolist()
            except sqlite3.Error as e:
                logging.error(f"Error fetching stock list: {e}")
                return []
        
        signals_list = []
        for symbol in symbols:
            logging.info(f"Analyzing {symbol}...")
            signals = self.analyze_stock(symbol=symbol, show_chart=show_charts)
            if signals:
                signals_list.append(signals)
                
        return signals_list

    def save_signals_to_db(self, signals):
        """Save signals to the database
        
        Args:
            signals: Dictionary containing signal data
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not signals:
            return False
            
        try:
            # If AI components are available, use the AI signal generator to save
            if self.use_ai and hasattr(self, 'ai_signals'):
                return self.ai_signals.save_signal_to_db(signals)
                
            # Otherwise, save using standard method
            if not self.conn:
                if not self.connect_db():
                    return False
                    
            cursor = self.conn.cursor()
            
            # Get stock_id for the symbol
            cursor.execute("SELECT id FROM stocks WHERE symbol = ?", (signals.get('symbol'),))
            result = cursor.fetchone()
            
            if not result:
                logging.warning(f"Stock not found for symbol: {signals.get('symbol')}")
                return False
                
            stock_id = result[0]
            
            # Check if this signal already exists for this date
            cursor.execute(
                "SELECT id FROM stock_signals WHERE symbol = ? AND signal_date = ?", 
                (signals.get('symbol'), signals.get('date'))
            )
            
            existing_signal = cursor.fetchone()
            
            # Prepare notes containing additional signal data
            notes = f"MA: {signals.get('ma_signal_desc')}, RSI: {signals.get('rsi_signal_desc')}, "
            notes += f"Price vs MA: {signals.get('price_vs_ma')}, RSI: {signals.get('rsi'):.2f}, "
            notes += f"Change: {signals.get('change_percent'):.2f}%"
            
            # Prepare query parameters
            params = (
                stock_id,
                signals.get('symbol'),
                signals.get('date'),
                signals.get('ma_signal_desc'),  # Using MA signal as the ai_signal
                signals.get('confidence', 100) if 'confidence' in signals else 75,  # Default confidence
                0.0,  # Default ai_score
                signals.get('close'),
                signals.get('rsi'),
                signals.get('ma_50'),  # Using MA as sma20
                0.0,  # Default sma50
                signals.get('combined_signal', 0),
                signals.get('combined_signal_desc'),
                notes
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
                
                logging.info(f"Updated signal for {signals.get('symbol')} on {signals.get('date')}")
            else:
                # Insert new signal
                cursor.execute("""
                    INSERT INTO stock_signals (
                        stock_id, symbol, signal_date, ai_signal, confidence, ai_score,
                        close, rsi, sma20, sma50, combined_signal, combined_signal_desc, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, params)
                
                logging.info(f"Inserted new signal for {signals.get('symbol')} on {signals.get('date')}")
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logging.error(f"Error saving signals to database: {e}", exc_info=True)
            return False

def main():
    parser = argparse.ArgumentParser(description='Generate technical trading signals')
    parser.add_argument('--symbol', help='Stock symbol to analyze')
    parser.add_argument('--days', type=int, default=100, help='Number of days of historical data')
    parser.add_argument('--list', action='store_true', help='Analyze all available stocks')
    parser.add_argument('--no-chart', action='store_true', help='Do not show charts')
    
    args = parser.parse_args()
    
    signal_gen = SignalGenerator()
    
    try:
        if args.list:
            # Analyze top stocks
            signals_list = signal_gen.analyze_multiple_stocks(show_charts=not args.no_chart)
            signal_gen.print_signals_summary(signals_list)
        elif args.symbol:
            # Analyze single stock
            signals = signal_gen.analyze_stock(symbol=args.symbol, days=args.days, show_chart=not args.no_chart)
            if signals:
                signal_gen.print_signals_summary([signals])
        else:
            # No arguments provided, show help
            parser.print_help()
    finally:
        signal_gen.close_db()

if __name__ == "__main__":
    main()