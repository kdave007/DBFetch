import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.format_controller import FormatController
from controllers.dbf_controller import DBFController
from models.db_connection import DBConnection

def test_filtered():
    # 1. Database configuration (hardcoded for testing)
    db_config = {
        'dbname': 'simcare',
        'user': 'dbuser',
        'password': 'comexcare',
        'host': 'localhost',
        'port': '5432'
    }

    # 2. DBF file configuration
    file_name = 'CANCFDI'  # Change this to your DBF file name
    test_file = f"../mockDBF/{file_name}.dbf"

    # If True, execute the SQL statements in the PostgreSQL database
    # If False, only print the SQL statements to the console
    preview_sql = True

    try:
        # 3. Initialize controllers
        dbf_controller = DBFController()
        format_controller = FormatController()

        
        if os.path.exists(test_file):
            # 5. Read DBF file
            print(f"\nProcessing DBF file: {test_file}")
            dbf_controller.set_dbf_file(test_file,30)

            print("\n3. Testing Date Range Filter")
            date_condition = [{
                'field': 'NO_REFEREN',  # Adjust field name as needed
                'value': '300000',
                'value2': '300300',
                'operator': 'between'
            }
            
            ]

            # 6. Get records and fields
            records = dbf_controller.get_filtered_records(date_condition,30)  # Adjust limit as needed
            fields = dbf_controller.dbf_reader.get_field_info()
            
            

            format_controller = FormatController()
            sql_statements = format_controller.format_to_sql(
                table_name=file_name,
                fields=fields,
                records=records,
                batch_size=10  # Small batch size for testing
            )

            format_controller.save_sql_file(sql_statements,'../',file_name)

            # 4. Set up database connection
            db = DBConnection(db_config)

            for i, statement in enumerate(sql_statements):
                print(f"\nExecuting statement {i}/{len(sql_statements)}")
                result = db.execute_query(statement)
                if result is not None:
                    print(f"Statement {i} executed successfully")
                else:
                    print(f"Statement {i} failed")


    except Exception as e:
        print(f"Error during execution: {e}")
    finally:
        # 10. Clean up
        if 'db' in locals():
            db.close_pool()
       

if __name__ == "__main__":
    test_filtered()