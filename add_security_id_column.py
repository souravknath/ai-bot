import os
import sys
import logging
from datetime import datetime
from db_handler import DatabaseHandler

# Set up logging
log_filename = f"add_security_id_column_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    logging.info("Starting security_id column addition process")
    
    try:
        # Initialize database connection
        db = DatabaseHandler()
        if not db.connect():
            logging.error("Failed to connect to database")
            return False
        
        # Add security_id column to history_data table
        result = db.add_security_id_to_history_data()
        
        if result:
            logging.info("Successfully added security_id column to history_data table")
        else:
            logging.info("No changes made - security_id column may already exist")
            
        # Close database connection
        db.close()
        
        return result
    
    except Exception as e:
        logging.error(f"Error during security_id column addition: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        logging.info("Process completed successfully")
    else:
        logging.info("Process completed with errors or no changes needed")