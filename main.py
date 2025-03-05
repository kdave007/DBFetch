#!/usr/bin/env python3

import sys
import os
from controllers.dbf_to_sql_controller import DbftoSqlController
import logging
from datetime import datetime
from config import (
    get_dbf_directory, 
    get_sql_output_directory,
    get_filter_conditions,
    get_max_records,
    is_filtering_enabled,
    FEATURE_FLAGS
)

def setup_logging():
    """Configure logging to both file and console"""
    # Create logs directory if it doesn't exist
    from pathlib import Path
    Path("logs").mkdir(exist_ok=True)
    
    # Setup logging format
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(f"logs/dbforge_{datetime.now().strftime('%Y%m%d')}.log"),
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    # Setup logging
    setup_logging()
    logging.info("=================== Starting DBFetch ===================")
    logging.info(f"Feature Flags Configuration: {FEATURE_FLAGS}")
    
    toSql = None
    try:
        # Get paths from config
        dbf_path = get_dbf_directory()
        output_path = get_sql_output_directory()
        logging.info(f"DBF Directory: {dbf_path}")
        logging.info(f"Output Directory: {output_path}")
        
        # Get filter conditions if enabled
        conditions = get_filter_conditions() if is_filtering_enabled() else []
        if conditions:
            logging.info(f"Filter conditions enabled: {conditions}")
        else:
            logging.info("No filter conditions applied")
        
        # Get max records from config
        max_records = get_max_records()
        logging.info(f"Max records limit: {max_records}")
        
        # Initialize controller
        logging.info("Initializing DBF to SQL controller...")
        toSql = DbftoSqlController()
        
        # Configure and start processing
        logging.info("Configuring controller and starting processing...")
        toSql.set_config(
            file_path=dbf_path,
            preview_path=output_path,
            conditions=conditions,
            max_records=max_records
        )
        toSql.process_start()
        logging.info("Processing completed successfully")

    except Exception as e:
        logging.error(f"Critical error during execution: {str(e)}", exc_info=True)
    finally:
        # Always cleanup resources
        if toSql:
            try:
                toSql.cleanup()
                logging.info("=============== Cleanup completed ===============")
            except Exception as e:
                logging.error(f"Error during cleanup: {str(e)}", exc_info=True)
        logging.info("==================== DBFetch End ====================\n")

if __name__ == "__main__":
    main()