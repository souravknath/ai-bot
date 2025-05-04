#!/usr/bin/env python
"""
This script runs the automated order placement system.
It can be scheduled to run daily after market close to analyze signals
and place orders for the next trading day.
"""

import logging
import sys
import os
import time
import json
from datetime import datetime, timedelta
import schedule
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f"auto_orders_scheduler_{datetime.now().strftime('%Y%m%d')}.log",
    filemode='a'
)

# Add console handler
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

def run_order_system():
    """Run the auto order system"""
    logging.info("Starting automated order system...")
    
    try:
        from auto_order import AutoOrderPlacer
        
        # Create auto order placer instance
        order_placer = AutoOrderPlacer()
        
        # Process signals and place orders
        order_placer.process_signals()
        
        logging.info("Automated order process completed")
    except Exception as e:
        logging.error(f"Error running auto order system: {e}", exc_info=True)

def check_if_trading_day():
    """Check if today is a trading day (simple implementation)"""
    today = datetime.now()
    
    # Skip weekends (0 = Monday, 6 = Sunday in Python's datetime)
    if today.weekday() >= 5:  # Saturday or Sunday
        logging.info(f"Today is {today.strftime('%A')} - not a trading day")
        return False
    
    # TODO: Check for holidays and other non-trading days
    # This would require a holiday calendar or API to check
    
    return True

def run_scheduled_job():
    """Run the scheduled job if it's a trading day"""
    if check_if_trading_day():
        run_order_system()
    else:
        logging.info("Not a trading day - skipping order processing")

def main():
    """Main function to schedule and run the auto order system"""
    # Get the script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Change to script directory to ensure relative paths work
    os.chdir(script_dir)
    
    # First run immediately
    logging.info("Auto order scheduler started")
    run_scheduled_job()
    
    # Schedule to run at specified time (e.g., after market close)
    schedule.every().day.at("16:30").do(run_scheduled_job)  # 4:30 PM
    
    # Also run once in the morning before market open
    schedule.every().day.at("08:30").do(run_scheduled_job)  # 8:30 AM
    
    logging.info("Scheduler set up - will run daily at 8:30 AM and 4:30 PM")
    
    try:
        # Keep the scheduler running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logging.info("Scheduler stopped by user")
    except Exception as e:
        logging.error(f"Scheduler error: {e}", exc_info=True)

if __name__ == "__main__":
    main()