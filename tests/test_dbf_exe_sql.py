import sys
import os
from datetime import datetime

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.dbf_controller import DBFController
from controllers.format_controller import FormatController
from models.db_connection import DBConnection

def test_dbf_to_sql_execution():
    # 1. Database configuration (hardcoded for testing)
    db_config = {
        'dbname': 'simcare',
        'user': 'dbuser',
        'password': 'comexcare',
        'host': 'localhost',
        'port': '5432'
    }

    # 2. DBF file configuration
    file_name = 'VALES'  # Change this to your DBF file name
    test_file = f"../mockDBF/{file_name}.dbf"

    # If True, execute the SQL statements in the PostgreSQL database
    # If False, only print the SQL statements to the console
    execute_sql = True

    try:
        # 3. Initialize controllers
        dbf_controller = DBFController()
        format_controller = FormatController()
        
        # 4. Set up database connection
        db = DBConnection(db_config)
        
        if os.path.exists(test_file):
            # 5. Read DBF file
            print(f"\nProcessing DBF file: {test_file}")
            dbf_controller.set_dbf_file(test_file,4000)
            
            # 6. Get records and fields
            records = dbf_controller.get_last_records(limit=1400)  # Adjust limit as needed
            fields = dbf_controller.dbf_reader.get_field_info()
            
            if records and fields:
                print(f"Retrieved {len(records)} records")
                
                # 7. Generate SQL statements
                sql_statements = format_controller.format_to_sql(
                    table_name=file_name,
                    fields=fields,
                    records=records,
                    batch_size=50
                )
                
                # 8. Save SQL to file
                #format_controller.save_sql_file(sql_statements, '../', file_name)
                
                
                # 9. Execute SQL statements
                if execute_sql:
                    print("\nExecuting SQL statements...")
                    for i, statement in enumerate(sql_statements, 1):
                        print(f"\nExecuting statement {i}/{len(sql_statements)}")
                        result = db.execute_query(statement)
                        if result is not None:
                            print(f"Statement {i} executed successfully")
                        else:
                            print(f"Statement {i} failed")
                
                print("\nAll operations completed")
            else:
                print("No records or fields found in DBF file")
        else:
            print(f"DBF file not found: {test_file}")
            
    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        # 10. Clean up
        if 'db' in locals():
            db.close_pool()

if __name__ == "__main__":
    test_dbf_to_sql_execution()