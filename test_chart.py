#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sqlite3
import logging
import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot
import webbrowser
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("chart_test.log")
    ]
)

def test_chart_generation():
    """Test the chart generation functionality"""
    try:
        # Connect to the database
        logging.info("Connecting to database...")
        conn = sqlite3.connect("stock_data.db")
        
        # Get a stock to test with (first stock in the database)
        query = "SELECT id, security_id, symbol, name FROM stocks LIMIT 1"
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        
        if not result:
            logging.error("No stocks found in the database")
            return False
            
        stock_id, security_id, symbol, name = result
        logging.info(f"Testing with stock: {symbol} (ID: {stock_id}, Security ID: {security_id})")
        
        # Calculate date range (last 30 days)
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=30)
        from_date = start_date.strftime("%Y-%m-%d")
        to_date = end_date.strftime("%Y-%m-%d")
        
        logging.info(f"Date range: {from_date} to {to_date}")
        
        # Query for historical data
        logging.info("Querying for historical data...")
        query = """
            SELECT timestamp, date, open, high, low, close, volume
            FROM history_data
            WHERE stock_id = ? 
            AND date BETWEEN ? AND ?
            ORDER BY timestamp
        """
        df = pd.read_sql_query(query, conn, params=(stock_id, from_date, to_date))
        logging.info(f"Found {len(df)} records for stock_id {stock_id}")
        
        # If no records with stock_id, try security_id
        if len(df) < 2:
            logging.info(f"Trying with security_id: {security_id}")
            query = """
                SELECT timestamp, date, open, high, low, close, volume
                FROM history_data
                WHERE security_id = ? 
                AND date BETWEEN ? AND ?
                ORDER BY timestamp
            """
            df = pd.read_sql_query(query, conn, params=(security_id, from_date, to_date))
            logging.info(f"Found {len(df)} records for security_id {security_id}")
        
        # Check if we have enough data
        if len(df) < 2:
            logging.error(f"Insufficient data for chart: only {len(df)} records available")
            return False
        
        logging.info(f"DataFrame columns: {df.columns.tolist()}")
        logging.info(f"First few rows: {df.head().to_dict()}")
        
        # For charting, use the date column if available, otherwise use timestamp
        if 'date' in df.columns:
            logging.info("Using 'date' column for x-axis")
            if isinstance(df['date'].iloc[0], str):
                logging.info("Converting string dates to datetime")
                df['date'] = pd.to_datetime(df['date'])
            x_values = df['date']
        else:
            logging.info("Using 'timestamp' column for x-axis")
            x_values = pd.to_datetime(df['timestamp'])
        
        # Create candlestick chart
        logging.info("Creating chart figure...")
        fig = go.Figure()
        
        # Add candlestick trace
        fig.add_trace(
            go.Candlestick(
                x=x_values,
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name=symbol
            )
        )
        
        # Add volume as bar chart on secondary y-axis
        fig.add_trace(
            go.Bar(
                x=x_values,
                y=df['volume'],
                name="Volume",
                marker_color='rgba(128,128,128,0.5)',
                opacity=0.5,
                yaxis="y2"
            )
        )
        
        # Layout adjustments
        fig.update_layout(
            title=f"{name} ({symbol}) - 30 Day Chart (TEST)",
            xaxis_title="Date",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
            yaxis2=dict(
                title="Volume",
                overlaying="y",
                side="right",
                showgrid=False
            ),
            height=800
        )
        
        # Save the HTML file
        chart_dir = "charts"
        if not os.path.exists(chart_dir):
            os.makedirs(chart_dir)
        
        filename = f"{chart_dir}/{symbol}_test_{from_date}_{to_date}.html"
        logging.info(f"Saving chart to {filename}...")
        plot(fig, filename=filename, auto_open=False)
        
        # Open browser
        logging.info(f"Opening chart in browser...")
        webbrowser.open('file://' + os.path.abspath(filename))
        
        logging.info("Chart test completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error in test_chart_generation: {str(e)}", exc_info=True)
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    success = test_chart_generation()
    print(f"Chart generation test {'succeeded' if success else 'failed'}")