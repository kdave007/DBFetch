import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.dbf_controller import DBFController

def run_tests():
    # Initialize the controller
    print("\n=== Testing DBF Controller ===")
    controller = DBFController()
    
    # Test 1: Check initialization
    print("\nTest 1: Controller Initialization")
    print(f"DBF Reader is None: {controller.dbf_reader is None}")
    
    # Test 2: Try to set invalid file
    print("\nTest 2: Setting Invalid DBF File")
    result = controller.set_dbf_file("nonexistent.dbf", 1000)
    print(f"Set invalid file result (should be False): {result}")
    
    # Test 3: Set valid DBF file
    # Replace this path with your actual DBF file path
    test_file = "../mockDBF/CANCFDI.dbf"
    
    if os.path.exists(test_file):
        print(f"\nTest 3: Setting Valid DBF File: {test_file}")
        result = controller.set_dbf_file(test_file, 1000)
        print(f"Set valid file result (should be True): {result}")
        
        if result:
            # Test 4: Get record count
            print("\nTest 4: Getting Record Count")
            count = controller.get_record_count()
            print(f"Total records in file: {count}")
            
            # Test 5: Get first 5 records
            print("\nTest 5: Getting First 5 Records")
            records = controller.get_records(5)
            if records:
                print(f"Number of records retrieved: {len(records)}")
                for record in records:
                    print("Record fields:", list(record.values()))
            
            # Test 6: Get last 3 records
            print("\nTest 6: Getting Last 3 Records")
            last_records = controller.get_last_records(3)
            if last_records:
                print(f"Number of last records retrieved: {len(last_records)}")
                for last_record in last_records:
                    print("Record fields:", list(last_record.values()))
    else:
        print(f"\nSkipping file-based tests: Test file not found at {test_file}")

if __name__ == "__main__":
    run_tests()