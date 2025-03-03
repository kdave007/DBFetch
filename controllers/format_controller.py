from models.sql_formatter import SQLFormatter
from datetime import datetime
import os

class FormatController:
    def __init__(self) -> None:
        pass

    
    def format_to_sql(self, table_name, records, fields,  batch_size: int = 1000):
        sql_f = SQLFormatter()
        return sql_f.create_insert_statements(table_name, records, fields,  batch_size)
    
    def save_sql_file(self, sql_statements: list, output_path: str, table_name: str):
        """Save SQL statements to a .sql file"""
        
        now = datetime.now()
        file_name = f"{table_name}_{now.strftime('%Y-%m-%d_%H-%M-%S')}.sql"
        file_path = os.path.join(output_path, file_name)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for statement in sql_statements:
                f.write(statement + ';\n')
        print(f"File saved at: {file_path}")
    
    def format_to_json(self, records):
        """Convert records to JSON string"""
        pass
    
    def format_to_txt(self, records, fields):
        """Convert records to formatted text"""
        pass
    
    def save_formatted_data(self, formatted_data, output_path):
        """Save formatted data to a file"""
        pass
