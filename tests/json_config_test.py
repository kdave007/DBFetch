import sys
import os
from pathlib import Path

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.dbf_controller import DBFController
from controllers.format_controller import FormatController
from config import get_filter_conditions, is_filtering_enabled

def test_filter_config():
    print("\n=== Testing Filter Configuration ===")
    
    # Initialize controller
    dbf_controller = DBFController()
    
    # Test file path - adjust as needed
    file_name = 'CANCFDI'  # Change this to your DBF file name
    test_file = f"../mockDBF/{file_name}.dbf"
    
    if not os.path.exists(test_file):
        print(f"Test file not found: {test_file}")
        return
    
    # Set up DBF file
    if not dbf_controller.set_dbf_file(test_file, 40):
        print("Failed to set DBF file")
        return
    
    try:
        # Print current filter configuration
        print("\n1. Current Filter Configuration:")
        if is_filtering_enabled():
            try:
                conditions = get_filter_conditions()
                print("Filtering is ENABLED")
                print("Active conditions:", conditions)
            except ValueError as e:
                print(f"Error in filter configuration: {e}")
                return  # Exit early on filter error
        else:
            print("Filtering is DISABLED")
            return  # Exit if filtering is disabled
        
        # Get and display filtered records
        print("\n2. Filtered Records:")
        records = dbf_controller.get_filtered_records(conditions,30)  # Adjust limit as needed
        fields = dbf_controller.dbf_reader.get_field_info()
        
        

        format_controller = FormatController()
        sql_statements = format_controller.format_to_sql(
            table_name=file_name,
            fields=fields,
            records=records,
            batch_size=10  # Small batch size for testing
        )

        format_controller.save_sql_file(sql_statements,'../',file_name)
    except Exception as e :
        print("Error ")

if __name__ == "__main__":
    test_filter_config()