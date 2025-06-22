#!/usr/bin/env python3
"""
Test script to verify the database type fix works correctly.
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

def test_database_fix():
    """Test the database fix with sample data."""
    try:
        # Initialize the database
        db = FlightDataDB()
        session_id = "test_session"
        
        # Sample data that might cause the original issue
        sample_data = {
            "GPS_RAW_INT": {
                "time_usec": [608463000, 608464000, 608465000],
                "lat": [37.7749, 37.7750, 37.7751],
                "lon": [-122.4194, -122.4195, -122.4196],
                "alt": [100, 101, 102],
                "vel": [10, 11, 12],
                "satellites_visible": [8, 9, 10]
            },
            "GLOBAL_POSITION_INT": {
                "time_boot_ms": [608463000, 608464000, 608465000],
                "lat": [37.7749, 37.7750, 37.7751],
                "lon": [-122.4194, -122.4195, -122.4196],
                "alt": [100, 101, 102],
                "relative_alt": [50, 51, 52],
                "vx": [1, 2, 3],
                "vy": [4, 5, 6],
                "vz": [7, 8, 9],
                "hdg": [180, 181, 182]
            }
        }
        
        # Store the sample data
        logger.info("Storing sample data...")
        db.store_flight_data(session_id, sample_data)
        
        # Test the problematic query
        logger.info("Testing the problematic query...")
        query = """
        SELECT 
            g.time_usec AS gps_time_usec, 
            g.lat AS gps_latitude, 
            g.lon AS gps_longitude, 
            g.alt AS gps_altitude, 
            g.vel AS gps_ground_speed, 
            g.satellites_visible AS gps_satellites_visible, 
            p.time_boot_ms AS global_time_boot_ms, 
            p.lat AS global_latitude, 
            p.lon AS global_longitude, 
            p.alt AS global_altitude, 
            p.relative_alt AS global_relative_altitude, 
            p.vx AS global_vx, 
            p.vy AS global_vy, 
            p.vz AS global_vz, 
            p.hdg AS global_heading
        FROM 
            GPS_RAW_INT g
        JOIN 
            GLOBAL_POSITION_INT p ON g.time_usec = p.time_boot_ms
        ORDER BY 
            g.time_usec
        """
        
        result = db.query(session_id, query)
        logger.info(f"Query executed successfully! Result has {len(result)} rows")
        logger.info(f"Result columns: {list(result.columns)}")
        
        if len(result) > 0:
            logger.info(f"First row: {result.iloc[0].to_dict()}")
        
        # Close the database
        db.close()
        
        logger.info("Test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_database_fix()
    sys.exit(0 if success else 1) 