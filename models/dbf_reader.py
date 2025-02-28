from dbfread import DBF
from typing import Dict, Optional, List, Tuple
import os

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
                        'decimal': field.decimal if hasattr(field, 'decimal') else 0
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
                        'decimal': field.decimal if hasattr(field, 'decimal') else 0
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