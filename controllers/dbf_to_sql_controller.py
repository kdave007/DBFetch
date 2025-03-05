from typing import Dict, List, Optional
import os
import logging
from controllers.dbf_controller import DBFController
from controllers.format_controller import FormatController
from models.db_connection import DBConnection
from config import DB_CONFIG, get_preview_mode, is_execute_query_enabled, FEATURE_FLAGS

class DbftoSqlController():
    def __init__(self) -> None:
        self.dbf_controller = DBFController()
        self.initialized = False
        self.db_connection = None

    def set_config(self, file_path: str = None, preview_path: str = None, conditions: List[Dict[str, str]] = None, max_records: Optional[int] = None, batch_size: int = 1000):
        logging.info(f"Configuring DBF to SQL controller with:")
        logging.info(f"- File path: {file_path}")
        logging.info(f"- Preview path: {preview_path}")
        logging.info(f"- Filter conditions: {conditions}")
        logging.info(f"- Max records: {max_records}")
        
        self.file_path = file_path
        self.conditions = conditions if conditions else []
        self.max_records = max_records
        self.batch_size = batch_size
        self.preview_path = preview_path

        #print(DB_CONFIG)
        #print(FEATURE_FLAGS)

        self.initialized = True
        return self

    def _get_db_connection(self) -> Optional[DBConnection]:
        """
        Get or create a database connection.
        """
        try:
            if self.db_connection is None:
                logging.info("Creating new database connection...")
                self.db_connection = DBConnection(DB_CONFIG, 1, 1)
            else:
                logging.info("Ensuring existing connection pool is open...")
                self.db_connection.ensure_pool_is_open()
            return self.db_connection
        except Exception as e:
            logging.error(f"Failed to get database connection: {e}", exc_info=True)
            return None

    def cleanup(self):
        """
        Clean up resources when done with the controller.
        Call this method when you're completely done with database operations.
        """
        if self.db_connection and self.db_connection._pool:
            logging.info("Closing database connection pool...")
            self.db_connection.close_pool()
            self.db_connection = None
            logging.info("Database connection pool closed successfully")

    def process_start(self):
        if not self.initialized:
            logging.error("Controller not initialized - must call set_config first")
            raise ValueError("Must set_config values first...")

        try:
            logging.info("Starting DBF processing...")
            if os.path.exists(self.file_path):
                result = self.dbf_controller.set_dbf_file(self.file_path, self.max_records)

                if result:
                    # Get some records and fields
                    records = self.dbf_controller.get_filtered_records(self.conditions,self.max_records) 
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
                        logging.info(f"Saving SQL preview to: {self.preview_path}")

                        if FEATURE_FLAGS['preview_ext'] == 'sql':
                            format_controller.save_to_sql(sql_statements, self.preview_path, table_name)
                        else :
                            format_controller.save_to_txt(sql_statements, self.preview_path, table_name)

                        logging.info("SQL preview saved successfully")

                    # Execute SQL if enabled in config and connection is available
                    
                    if is_execute_query_enabled() :
                        try:

                            logging.info("Executing SQL statements...")
                            db = self._get_db_connection()
                            for i, statement in enumerate(sql_statements, 1):
                                db.execute_query(statement)
                                if i % 100 == 0:  # Log progress every 100 statements
                                    logging.info(f"Executed {i}/{len(sql_statements)} SQL statements")
                            logging.info(f"Successfully executed all SQL statements for table {table_name}")
                        except Exception as e:
                            logging.error(f"Error executing SQL: {e}", exc_info=True)
                    else:
                        logging.info("SQL execution skipped - not enabled in config")

            logging.info("DBF processing completed successfully")
            
        except Exception as e:
            logging.error(f"Error during DBF processing: {e}", exc_info=True)
            raise
