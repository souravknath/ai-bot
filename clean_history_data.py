#!/usr/bin/env python3
"""
Script to clean the history_data table in the stock database
"""

import argparse
import logging
from db_handler import DatabaseHandler
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"history_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

def main():
    """Main function to clean history data"""
    parser = argparse.ArgumentParser(description='Clean history_data table in stock database')
    
    # Add command line arguments
    parser.add_argument('--days', type=int, help='Delete data older than this many days')
    parser.add_argument('--stock-id', type=int, help='Delete data for a specific stock ID')
    parser.add_argument('--before-date', help='Delete data before this date (format: YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='Delete all data from history_data table')
    
    args = parser.parse_args()

    # Initialize database handler
    db_handler = DatabaseHandler()
    if not db_handler.connect():
        logging.error("Failed to connect to database")
        return

    try:
        # Check if any cleaning options were provided
        if not any([args.days, args.stock_id, args.before_date, args.all]):
            logging.error("No cleaning options provided. Use --help for available options")
            return
        
        # Confirm before deleting all data
        if args.all and input("Are you sure you want to delete ALL history data? (y/N): ").lower() != 'y':
            logging.info("Operation cancelled")
            return
        
        # Clean the data
        records_deleted = db_handler.clean_history_data(
            older_than_days=args.days,
            stock_id=args.stock_id,
            before_date=args.before_date,
            all_data=args.all
        )
        
        logging.info(f"Cleaning completed successfully. {records_deleted} records deleted.")
        
    finally:
        # Close database connection
        db_handler.close()

if __name__ == "__main__":
    main()