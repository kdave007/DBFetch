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

    def export_to_json(self, records: List[Dict]) -> str:
        """Convert records to JSON string"""

    def export_to_txt(self, records: List[Dict]) -> str:
        """Convert records to formatted text"""

    def export_to_sql(self, records: List[Dict], table_name: str = None) -> str:
        """Convert records to SQL INSERT statements"""