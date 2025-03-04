from dbfread import DBF
from typing import Dict, Optional, List, Tuple
import os
from datetime import datetime, date

class DBFReader:
    def __init__(self, file_path: str, max_records: int = 1000):
        """
        Initialize DBF reader with file path and maximum records limit
        
        Args:
            file_path (str): Path to the DBF file
            max_records (int): Maximum number of records to load in memory
        """
        self.max_records = max_records if max_records and (max_records < 30000)  else 30000
        self.file_path = file_path 
        self.memo_files = self._find_memo_files()
        

    def validate_file(self) -> bool:
        """
        Validate if the file exists, has .dbf extension and can be opened.
        Also checks for associated memo files if they exist.
        
        Returns:
            bool: True if valid, False otherwise
        """
        if not os.path.exists(self.file_path) :
            print(f"Error: File does not exist: {self.file_path}")
            return False
        
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                # If we have memo files, verify they can be accessed
                for memo_type, memo_path in  self.memo_files.items():
                    if not os.path.exists(memo_path):
                        print(f"Warning: {memo_type.upper()} memo file exists but cannot be accessed: {memo_path}")
                        return False
                
                return True

        except Exception as e:
            print(f"Error reading DBF file: {e}")
            return False    


    def _find_memo_files(self) -> Dict[str, str]:
        """
        Find associated memo files (FPT, DBT) for the DBF file.
        
        Returns:
            Dict[str, str]: Dictionary with memo file types and their paths
        """
        memo_files = {}
        base_path = os.path.splitext(self.file_path)[0]
        
        # Check for FPT file
        fpt_path = f"{base_path}.fpt"
        if os.path.exists(fpt_path):
            memo_files['fpt'] = fpt_path
        
        # Check for DBT file
        dbt_path = f"{base_path}.dbt"
        if os.path.exists(dbt_path):
            memo_files['dbt'] = dbt_path
            
        return memo_files
    
    def has_memo_fields(self) -> bool:
        """
        Check if the DBF file has associated memo fields.
        
        Returns:
            bool: True if memo files exist, False otherwise
        """
        return len(self.memo_files) > 0

    def get_table_name(self) -> str:
        """
        Get the table name from the DBF file (usually the file name without extension).
        Makes the name database-friendly by:
        - Converting to lowercase
        - Replacing spaces with underscores
        - Removing special characters
        
        Returns:
            str: The sanitized table name
        """
        import re
        
        # Get the base filename without extension
        file_name = os.path.basename(self.file_path)
        table_name = os.path.splitext(file_name)[0]
        
        # Make it database-friendly
        table_name = table_name.lower()  # Convert to lowercase
        table_name = re.sub(r'\s+', '_', table_name)  # Replace spaces with underscores
        table_name = re.sub(r'[^a-z0-9_]', '', table_name)  # Remove special chars
        
        return table_name    

    def get_N_records(self, limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Get records from the DBF file with optional start position and limit
        
        Args:
            limit (int): Number of records to retrieve
            
        Returns:
            list: List of dictionaries containing the records
        """
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                records = list(dbf)
                if limit is None:
                    limit = self.max_records
                limit = min( limit, len(records)) 
                limit = min( limit, self.max_records)
                records = records[:limit]
                return records            

        except Exception as e:
            print(f"Error reading DBF file: {e}")
            return None
        

    def get_last_n_records(self, n: int) -> list:
        """
        Get the last N records from the DBF file
        
        Args:
            n (int): Number of records to retrieve from the end
            
        Returns:
            list: List of dictionaries containing the last N records
        """
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                records = list(dbf)
                n = min( n, len(records)) 
                n = min( n, self.max_records)
                last_records = records[-n:]
                return last_records            

        except Exception as e:
            print(f"Error reading DBF file: {e}")
            return None

    def get_total_records(self) -> int:
        """
        Get total number of records in the DBF file
        
        Returns:
            int: Total number of records
        """
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                return len(dbf)

        except Exception as e:
            print(f"Error reading DBF file: {e}")
            return 0

    def get_field_info(self) -> List[Dict[str, str]]:
        """
        Get information about the fields in the DBF file
        
        Returns:
            List[Dict[str, str]]: List of dictionaries containing field information
                Each dictionary contains:
                - name: Field name
                - type: Field type (C for Character, N for Numeric, etc.)
                - length: Field length
        """
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                field_info = []
                
                # Iterate through each field and collect its information
                for field in dbf.fields:
                    field_data = {
                        'name': field.name,
                        'type': field.type,
                        'length': field.length,
                        'decimal_count': field.decimal_count if hasattr(field, 'decimal_count') else 0
                    }
                    field_info.append(field_data)
                    
                return field_info

        except Exception as e:
            print(f"Error reading DBF fields: {e}")
            return []

    def get_records_with_fields(self, limit: Optional[int] = None) -> Tuple[List[Dict], List[Dict[str, str]]]:
        """
        Get records along with field information
        
        Args:
            limit (int, optional): Number of records to retrieve
            
        Returns:
            Tuple[List[Dict], List[Dict[str, str]]]: Tuple containing:
                - List of record dictionaries
                - List of field information dictionaries
        """
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                # Get field information
                field_info = []
                for field in dbf.fields:
                    field_data = {
                        'name': field.name,
                        'type': field.type,
                        'length': field.length,
                        'decimal_count': field.decimal_count if hasattr(field, 'decimal_count') else 0
                    }
                    field_info.append(field_data)
                
                # Get records with limit
                records = list(dbf)
                if limit is None:
                    limit = self.max_records
                limit = min(limit, len(records))
                limit = min(limit, self.max_records)
                records = records[:limit]
                
                return records, field_info

        except Exception as e:
            print(f"Error reading DBF file: {e}")
            return [], []

    def get_filtered_records(self, conditions: List[Dict], limit: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Get records that match specified field conditions.
        
        Args:
            conditions: List of condition dictionaries. Each condition should have:
                - field: Field name to check
                - value: Value to match
                - operator: Optional operator ('=', '>', '<', '>=', '<=', 'between')
                - value2: Optional second value for 'between' operator
            limit: Optional maximum number of records to return
            
        Examples:
            # Get records where STATUS = 'A'
            reader.get_filtered_records([{'field': 'STATUS', 'value': 'A'}])
            
            # Get records where AMOUNT > 1000
            reader.get_filtered_records([{'field': 'AMOUNT', 'value': 1000, 'operator': '>'}])
            
            # Get records where DATE between '2024-01-01' and '2024-12-31'
            reader.get_filtered_records([{
                'field': 'DATE',
                'value': '2024-01-01',
                'value2': '2024-12-31',
                'operator': 'between'
            }])
            
            # Multiple conditions (all must match)
            reader.get_filtered_records([
                {'field': 'STATUS', 'value': 'A'},
                {'field': 'AMOUNT', 'value': 1000, 'operator': '>'}
            ])
        
        Returns:
            List of matching records, or None if error occurs
        """
        try:
            with DBF(self.file_path, encoding='latin1') as dbf:
                # Validate field names first
                field_names = [field.name for field in dbf.fields]
                for condition in conditions:
                    if condition['field'] not in field_names:
                        raise ValueError(f"Field '{condition['field']}' not found in DBF file")
                
                # Filter records
                filtered_records = []
                for record in dbf:
                    if self._matches_conditions(record, conditions):
                        filtered_records.append(record)
                        if limit and len(filtered_records) >= limit:
                            break
                
                return filtered_records

        except Exception as e:
            print(f"Error filtering records: {e}")
            return None

    def _matches_conditions(self, record: Dict, conditions: List[Dict]) -> bool:
        """
        Check if a record matches all specified conditions.
        
        Args:
            record: Single record to check
            conditions: List of condition dictionaries
            
        Returns:
            True if record matches all conditions, False otherwise
        """
        for condition in conditions:
            field = condition['field']
            value = condition['value']
            operator = condition.get('operator', '=')
            
            record_value = record[field]
            
            # Skip comparison if record value is None
            if record_value is None:
                return False

            # Convert dates if needed
            if isinstance(record_value, (datetime, date)):
                try:
                    # Convert string to date object
                    if isinstance(value, str):
                        parsed_date = datetime.strptime(value, '%Y-%m-%d')
                        value = parsed_date.date() if isinstance(record_value, date) else parsed_date
                        
                    if operator == 'between' and isinstance(condition['value2'], str):
                        parsed_date2 = datetime.strptime(condition['value2'], '%Y-%m-%d')
                        condition['value2'] = parsed_date2.date() if isinstance(record_value, date) else parsed_date2
                except ValueError as e:
                    print(f"Error parsing date value: {e}")
                    return False
            
            # Handle different operators
            if operator == '=':
                if record_value != value:
                    return False
            elif operator == '>':
                if not (record_value > value):
                    return False
            elif operator == '<':
                if not (record_value < value):
                    return False
            elif operator == '>=':
                if not (record_value >= value):
                    return False
            elif operator == '<=':
                if not (record_value <= value):
                    return False
            elif operator == 'between':
                value2 = condition['value2']
                if not (value <= record_value <= value2):
                    return False
        
        return True