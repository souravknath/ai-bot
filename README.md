# Stock Data Fetcher

This project fetches historical stock data from Dhan API and stores it in a SQLite database. It retrieves one year of historical data for up to 500 stocks.

## Features

- Creates a SQLite database with tables for stocks and historical data
- Fetches daily historical stock data from Dhan API
- Stores data for up to 500 stocks
- Handles API rate limits with exponential backoff

## Requirements

- Python 3.6+
- Required Python packages (see requirements.txt):
  - requests
  - python-dotenv
  - tqdm

## Setup

1. Clone this repository
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```
3. Create a `.env` file in the project root directory with your Dhan API key:
   ```
   DHAN_API_KEY=your_api_key_here
   ```
   - If you don't have an API key, you can [sign up](https://dhanhq.co/) and obtain one from Dhan

## Database Structure

The SQLite database (`stock_data.db`) contains two main tables:

1. **Stocks Table**
   - Contains basic information about each stock
   - Fields: id, security_id, exchange_segment, symbol, name, instrument, added_date, last_updated

2. **History Data Table**
   - Contains historical price and volume data for each stock
   - Fields: id, stock_id, timestamp, date, open, high, low, close, volume, open_interest
   - Foreign key relationship with stocks table

## Usage

Simply run the main.py script:
```
python main.py
```

This will:
1. Create a new SQLite database if it doesn't exist
2. Fetch a list of 500 stocks
3. For each stock, fetch one year of daily historical data
4. Store all data in the database

## Notes

- For demonstration purposes, the stock list includes only a few sample stocks. In a real implementation, you would fetch the actual list of stocks you want to track (e.g., Nifty 500 constituents).
- The Dhan API has rate limits, so the script includes delays between requests to avoid hitting these limits.
- Data from Dhan API is fetched in chunks as per their documentation.

## Files

- `main.py`: Main script to orchestrate the data fetching and storage
- `db_handler.py`: Handles database operations
- `stock_fetcher.py`: Handles API requests to fetch stock data
- `requirements.txt`: Lists required Python packages