from typing import Dict, List, Optional
from models.dbf_reader import DBFReader
import os

class DBFController:
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize controller with optional configuration
        
        Args:
            config: Configuration object (we'll implement this later)
        """
        self.dbf_reader = None  # Explicitly show it starts as None

    def set_dbf_file(self, file_path: str, max_records : int = 1000) -> bool:
        """
        Set and validate the DBF file to work with
        
        Args:
            file_path: Path to the DBF file
            
        Returns:
            bool: True if file is valid and ready to use
        """
        try:
            if not os.path.exists(file_path):
                print(f"Error: File does not exist: {file_path}")
                return False

            # Create and validate DBF reader
            self.dbf_reader = DBFReader(file_path, max_records)
            if not self.dbf_reader.validate_file():
                self.dbf_reader = None
                return False

            print(f"DBF file set successfully: {file_path}")
            return True

        except Exception as e:
            print(f"Error setting DBF file: {e}")
            self.dbf_reader = None
            return False

    def get_records(self, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Get first N records from the DBF file
        
        Args:
            limit (int, optional): Number of records to retrieve
            
        Returns:
            List[Dict]: List of records or None if error occurs
        """
        if not self.dbf_reader:
            print("Error: No DBF file has been set. Call set_dbf_file first.")
            return None
            
        try:
            return self.dbf_reader.get_N_records(limit)
        except Exception as e:
            print(f"Error getting records: {e}")
            return None

    def get_last_records(self, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Get last N records from the DBF file
        
        Args:
            limit (int, optional): Number of records to retrieve from the end
            
        Returns:
            List[Dict]: List of records or None if error occurs
        """
        if not self.dbf_reader:
            print("Error: No DBF file has been set. Call set_dbf_file first.")
            return None
            
        try:
            return self.dbf_reader.get_last_n_records(limit)
        except Exception as e:
            print(f"Error getting last records: {e}")
            return None

    def get_record_count(self) -> int:
        """
        Get total number of records in the DBF file
        
        Returns:
            int: Total number of records, 0 if error occurs
        """
        if not self.dbf_reader:
            print("Error: No DBF file has been set. Call set_dbf_file first.")
            return 0
            
        try:
            return self.dbf_reader.get_total_records()
        except Exception as e:
            print(f"Error getting record count: {e}")
            return 0

    def get_filtered_records(self, conditions: List[Dict], limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Get records that match specified field conditions
        
        Args:
            conditions: List of condition dictionaries. Each condition should have:
                - field: Field name to check
                - value: Value to match
                - operator: Optional operator ('=', '>', '<', '>=', '<=', 'between')
                - value2: Optional second value for 'between' operator
            limit: Optional maximum number of records to return
            
        Examples:
            # Get records where STATUS = 'A'
            controller.get_filtered_records([{'field': 'STATUS', 'value': 'A'}])
            
            # Get records where AMOUNT > 1000
            controller.get_filtered_records([{'field': 'AMOUNT', 'value': 1000, 'operator': '>'}])
            
            # Get records where DATE between '2024-01-01' and '2024-12-31'
            controller.get_filtered_records([{
                'field': 'DATE',
                'value': '2024-01-01',
                'value2': '2024-12-31',
                'operator': 'between'
            }])
            
            # Multiple conditions (all must match)
            controller.get_filtered_records([
                {'field': 'STATUS', 'value': 'A'},
                {'field': 'AMOUNT', 'value': 1000, 'operator': '>'}
            ])
            
        Returns:
            List[Dict]: List of filtered records or None if error occurs
        """
        if not self.dbf_reader:
            print("Error: No DBF file has been set. Call set_dbf_file first.")
            return None
        
        try:
            # Validate conditions format
            for condition in conditions:
                if 'field' not in condition or 'value' not in condition:
                    raise ValueError("Each condition must have 'field' and 'value' keys")
                
                operator = condition.get('operator', '=')
                if operator not in ['=', '>', '<', '>=', '<=', 'between']:
                    raise ValueError(f"Invalid operator: {operator}")
                
                if operator == 'between' and 'value2' not in condition:
                    raise ValueError("'between' operator requires 'value2' key")
            
            # Get filtered records
            return self.dbf_reader.get_filtered_records(conditions, limit)
            
        except Exception as e:
            print(f"Error getting filtered records: {e}")
            return None

    def get_field_info(self) -> List[Dict[str, str]]:
        """
        Get information about fields in the DBF file
        
        Returns:
            List[Dict]: List of field information dictionaries or empty list if error occurs
        """
        if not self.dbf_reader:
            print("Error: No DBF file has been set. Call set_dbf_file first.")
            return []
            
        try:
            return self.dbf_reader.get_field_info()
        except Exception as e:
            print(f"Error getting field info: {e}")
            return []

    @staticmethod
    def get_table_name(file_path: str) -> str:
        """
        Get table name from file path or config settings.
        If custom table name is enabled in config, use that.
        Otherwise, use sanitized file name.

        Args:
            file_path: Path to DBF file

        Returns:
            Table name to use for SQL
        """
        from config import is_custom_table_name_enabled, get_target_table_name

        if is_custom_table_name_enabled():
            return get_target_table_name()
        
        # Get file name without extension and path
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        
        # Sanitize for SQL:
        # - Convert to lowercase
        # - Replace spaces/special chars with underscore
        # - Remove any other invalid characters
        table_name = ''.join(c.lower() if c.isalnum() else '_' for c in file_name)
        return table_name.strip('_')  # Remove leading/trailing underscores