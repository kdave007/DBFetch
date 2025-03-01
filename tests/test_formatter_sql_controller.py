import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.format_controller import FormatController
from controllers.dbf_controller import DBFController

def test_sql_formatting():
    print("\n=== Testing Format Controller ===")
    
    # 1. Get some real DBF data first
    dbf_controller = DBFController()
    test_file = "../mockDBF/CANCFDI.dbf"  # Replace with your DBF file path
    
    if os.path.exists(test_file):
        # Set the DBF file
        dbf_controller.set_dbf_file(test_file)
        
        # Get some records and fields
        records = dbf_controller.get_last_records(limit=10)  # Get 5 records for testing
        fields = dbf_controller.dbf_reader.get_field_info()
        
        # 2. Format the data to SQL
        format_controller = FormatController()
        sql_statements = format_controller.format_to_sql(
            table_name="CANCFDI",
            fields=fields,
            records=records,
            batch_size=100  # Small batch size for testing
        )
        
        # 3. Print results
        print(f"\nGenerated {len(sql_statements)} SQL statements:")
        for i, statement in enumerate(sql_statements, 100):
            print(f"\nStatement {i}:")
            print(statement)
            print("-" * 80)
    else:
        print(f"\nSkipping tests: Test file not found at {test_file}")

if __name__ == "__main__":
    test_sql_formatting()