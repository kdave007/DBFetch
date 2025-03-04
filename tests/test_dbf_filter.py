import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from controllers.dbf_controller import DBFController

def test_filtered_records():
    print("\n=== Testing DBF Filter Functionality ===")
    controller = DBFController()
    
    # Test file path - adjust as needed
    test_file = "../mockDBF/CANCFDI.dbf"
    
    if not os.path.exists(test_file):
        print(f"Test file not found: {test_file}")
        return
    
    # Initialize controller with test file
    if not controller.set_dbf_file(test_file, 1000):
        print("Failed to set DBF file")
        return
        
    # Get and display field information
    print("\n1. Field Information:")
    fields = controller.get_field_info()
    for field in fields:
        print(f"Field: {field['name']:<15} Type: {field['type']} Length: {field['length']}")
    
    # Test single condition filter
    print("\n2. Testing Single Condition Filter")
    single_condition = [{'field': 'NO_REFEREN', 'value': '001028'}]  # Adjust field name as needed
    records = controller.get_filtered_records(single_condition, limit=5)
    if records:
        print(f"Found {len(records)} records")
        print("First record:", records[0])
    
    # Test date range filter
    print("\n3. Testing Date Range Filter")
    date_condition = [{
        'field': 'F_EMISION',  # Adjust field name as needed
        'value': '2021-01-01',
        'value2': '2021-12-31',
        'operator': 'between'
    }]
    date_records = controller.get_filtered_records(date_condition, limit=1000)
    if date_records:
        print(f"Found {len(date_records)} records in date range")
        for record in date_records:
            print( record['NO_REFEREN'])
    
    # Test multiple conditions
    print("\n4. Testing Multiple Conditions")
    multi_conditions = [
        {'field': 'STATUS', 'value': 'A'},
        {'field': 'MONTO', 'value': 1000, 'operator': '>'}  # Adjust field names as needed
    ]
    # multi_records = controller.get_filtered_records(multi_conditions, limit=5)
    # if multi_records:
    #     print(f"Found {len(multi_records)} records matching multiple conditions")
    #     print("First record:", multi_records[0])
    
    # Test error handling
    print("\n5. Testing Error Handling")
    # Invalid field
    print("\na. Invalid Field Test")
    invalid_field = [{'field': 'NONEXISTENT', 'value': 'test'}]
    result = controller.get_filtered_records(invalid_field)
    print(f"Invalid field test (should be None): {result is None}")
    
    # Invalid operator
    print("\nb. Invalid Operator Test")
    invalid_op = [{'field': 'STATUS', 'value': 'A', 'operator': 'invalid'}]
    result = controller.get_filtered_records(invalid_op)
    print(f"Invalid operator test (should be None): {result is None}")
    
    # Missing required field
    print("\nc. Missing Required Field Test")
    invalid_condition = [{'value': 'test'}]  # Missing 'field'
    result = controller.get_filtered_records(invalid_condition)
    print(f"Missing field test (should be None): {result is None}")

if __name__ == "__main__":
    test_filtered_records()