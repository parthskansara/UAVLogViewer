#!/usr/bin/env python3
"""
Script to fix database type issues in the FlightDataDB.
This script helps clean up existing databases that have type mismatches.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from tools.flight_data_db import FlightDataDB
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function to fix database type issues."""
    try:
        # Initialize the database
        db = FlightDataDB()
        
        # Get the session ID from command line or use a default
        session_id = sys.argv[1] if len(sys.argv) > 1 else "default_session"
        
        logger.info(f"Starting database cleanup for session: {session_id}")
        
        # Clean up existing data
        db.cleanup_existing_data(session_id)
        
        logger.info("Database cleanup completed successfully!")
        
        # Test a simple query to verify the fix
        try:
            # Try to get database information
            db_info = db.get_database_information(session_id)
            logger.info(f"Database contains {len(db_info)} tables")
            
            for table_name, info in db_info.items():
                logger.info(f"Table: {table_name}")
                if info['schema'] is not None:
                    logger.info(f"  Columns: {len(info['schema'])}")
        
        except Exception as e:
            logger.warning(f"Could not verify database structure: {str(e)}")
        
        # Close the database
        db.close()
        
    except Exception as e:
        logger.error(f"Error during database cleanup: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 