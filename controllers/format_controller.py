from models.sql_formatter import SQLFormatter
from datetime import datetime
import os

class FormatController:
    def __init__(self) -> None:
        pass

    
    def format_to_sql(self, table_name, records, fields,  batch_size: int = 1000):
        sql_f = SQLFormatter()
        return sql_f.create_insert_statements(table_name, records, fields,  batch_size)
    
    def _save_statements_to_file(self, sql_statements: list, output_path: str, table_name: str, extension: str):
        """
        Common method to save SQL statements to a file with specified extension
        
        Args:
            sql_statements: List of SQL statements to save
            output_path: Directory path to save the file
            table_name: Name of the table (used in filename)
            extension: File extension (without dot)
        """
        now = datetime.now()
        file_name = f"{table_name}_{now.strftime('%Y-%m-%d_%H-%M-%S')}.{extension}"
        file_path = os.path.join(output_path, file_name)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for statement in sql_statements:
                f.write(statement + ';\n')
        print(f"File saved at: {file_path}")
        return file_path

    def save_to_sql(self, sql_statements: list, output_path: str, table_name: str):
        """Save SQL statements to a .sql file"""
        return self._save_statements_to_file(sql_statements, output_path, table_name, 'sql')
    
    def format_to_json(self, records):
        """Convert records to JSON string"""
        pass
    
    def save_to_txt(self, sql_statements: list, output_path: str, table_name: str):
        """Save SQL statements to a .txt file"""
        return self._save_statements_to_file(sql_statements, output_path, table_name, 'txt')