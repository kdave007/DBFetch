#!/usr/bin/env python3

import sys
import os
from controllers.dbf_to_sql_controller import DbftoSqlController
from config import (
    get_dbf_directory, 
    get_sql_output_directory,
    get_filter_conditions,
    get_max_records,
    is_filtering_enabled
)

def main():
    try:
        toSql = DbftoSqlController()
        
        # Get paths from config
        dbf_path = get_dbf_directory()
        output_path = get_sql_output_directory()
        
        # Get filter conditions if enabled
        conditions = get_filter_conditions() if is_filtering_enabled() else []
        
        # Get max records from config
        max_records = get_max_records()
        
        # Configure and start processing
        toSql.set_config(
            file_path=dbf_path,
            preview_path=output_path,
            conditions=conditions,
            max_records=max_records
        )
        toSql.process_start()

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()