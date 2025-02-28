from models.sql_formatter import SQLFormatter

class FormatController:
    def __init__(self) -> None:
        pass

    
    def format_to_sql(self, table_name, records, fields,  batch_size: int = 1000):
        sql_f = SQLFormatter()
        return sql_f.create_insert_statements(table_name, records, fields,  batch_size)
    
    def format_to_json(self, records):
        """Convert records to JSON string"""
        pass
    
    def format_to_txt(self, records, fields):
        """Convert records to formatted text"""
        pass
    
    def save_formatted_data(self, formatted_data, output_path):
        """Save formatted data to a file"""
        pass
