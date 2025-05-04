#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from datetime import datetime
import sys
from db_handler import DatabaseHandler

# Configure logging
log_filename = f"update_security_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def main():
    """Update security_id column in history_data table for all entries"""
    logging.info("Starting security_id update process")
    
    try:
        # Initialize database connection
        db = DatabaseHandler()
        if not db.connect():
            logging.error("Failed to connect to database")
            return False
            
        # Create tables if they don't exist
        db.create_tables()
        
        # Update security_ids in history_data table
        updated_count = db.update_security_ids()
        
        logging.info(f"Security ID update completed. {updated_count} records updated.")
        db.close()
        return True
        
    except Exception as e:
        logging.error(f"Error during security ID update: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)