from typing import Dict, List, Optional
import os
from controllers.dbf_controller import DBFController
from controllers.format_controller import FormatController
from models.db_connection import DBConnection
from config import (
    get_config_value,
    CONFIG,
    SECURE_CONFIG,
    is_execute_query_enabled, 
    get_preview_mode
)

class DbftoSqlController():
    def __init__(self) -> None:
        self.dbf_controller = DBFController()
        self.initialized = False
        self.db_connection = None

    def set_config(self, file_path: str, preview_path: str, conditions: List[Dict[str, str]], max_records: Optional[int] = None, batch_size: int = 1000):
        self.file_path = file_path
        self.conditions = conditions
        self.max_records = max_records
        self.batch_size = batch_size
        self.preview_path = preview_path
    
        # Get database config from secure config
        db_config = {
            'host': SECURE_CONFIG.get('database', 'host'),
            'port': SECURE_CONFIG.getint('database', 'port'),
            'database': SECURE_CONFIG.get('database', 'database'),
            'user': SECURE_CONFIG.get('database', 'user'),
            'password': SECURE_CONFIG.get('database', 'password')
        }
        
        print("Using database config:", {k: '***' if k == 'password' else v for k, v in db_config.items()})
        self.db_connection = DBConnection(db_config)
        self.initialized = True
        return self

    def process_start(self):
        if not self.initialized:
            raise ValueError("Must set_config values first...")

        if os.path.exists(self.file_path):
            result = self.dbf_controller.set_dbf_file(self.file_path, self.max_records)

            if result:
                # Get some records and fields
                records = self.dbf_controller.get_filtered_records(self.conditions,30) 
                fields = self.dbf_controller.dbf_reader.get_field_info()

                table_name = DBFController.get_table_name(self.file_path)
            
                format_controller = FormatController()

                sql_statements = format_controller.format_to_sql(
                    table_name=table_name,
                    records=records,
                    fields=fields,
                    batch_size=self.batch_size
                )

                # Save SQL file
                if get_preview_mode():
                    format_controller.save_sql_file(sql_statements, self.preview_path, table_name)

                # Execute SQL if enabled in config and connection is available
                if is_execute_query_enabled() and self.db_connection:
                    try:
                        with self.db_connection.get_cursor() as cursor:
                            for statement in sql_statements:
                                cursor.execute(statement)
                            print(f"Successfully executed SQL statements for table {table_name}")
                    except Exception as e:
                        print(f"Error executing SQL: {e}")
                else:
                    print("SQL execution is disabled in config or no database connection available")

        return None
