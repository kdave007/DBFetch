import xml.sax.saxutils as saxutils
from typing import List, Dict, Any

class SQLFormatter:
    """
    Handles SQL formatting for DBF data.
    
    Attributes:
        None
        
    Methods:
        format_value: Format a single value based on DBF field type
        create_insert_statement: Create SQL INSERT statement from records
        _validate_table_name: Validate SQL table name
    """
    FIELD_TYPES = {
        'N': 'NUMERIC',
        'C': 'CHARACTER',
        'D': 'DATE',
        'M': 'MEMO',
        'L': 'LOGICAL'
    }

    @staticmethod
    def format_value(value, field_type: str, decimal: int = 0) -> str:
        """Format a single value based on its DBF field type"""
        if value is None:
            return 'NULL'
            
        if field_type == 'N':  # Numeric
            return str(value)
        elif field_type == 'D':  # Date
            return f"'{value}'"
        elif field_type == 'C':  # Character
            # Escape special characters and quotes for PostgreSQL
            escaped_value = str(value).replace("'", "''")
            escaped_value = escaped_value.replace('\\', '\\\\')
            return f"'{escaped_value}'"
        elif field_type == 'M':  # Memo/XML content
            if not value:
                return 'NULL'
            # 1. Convert to string
            value_str = str(value)
            # 2. Escape special characters for PostgreSQL
            escaped_value = value_str.replace("'", "''")  # Double single quotes
            escaped_value = escaped_value.replace('\\', '\\\\')  # Escape backslashes
            escaped_value = escaped_value.replace('\n', '\\n')  # Escape newlines
            escaped_value = escaped_value.replace('\r', '\\r')  # Escape carriage returns
            escaped_value = escaped_value.replace('\t', '\\t')  # Escape tabs
            # 3. Use E prefix for escaped string literals in PostgreSQL
            return f"E'{escaped_value}'"
        else:
            # Default handling for unknown field types
            escaped_value = str(value).replace("'", "''")
            escaped_value = escaped_value.replace('\\', '\\\\')
            return f"'{escaped_value}'"

    def create_insert_statements(self, table_name: str, records: List[Dict], fields: List[Dict],  batch_size: int = 1000) -> List[str]:
        """
        Create multiple SQL INSERT statements for batches of records
        
        Returns:
            List of SQL INSERT statements, one per batch
        """
        if not self._validate_table_name(table_name):
            raise ValueError(f'Invalid table name {table_name}')
        if not fields or not records:    
            raise ValueError('Fields or records cannot be empty')

        # Process records in batches
        # Process records in batches of size batch_size.
        # This prevents creating too many parameters in a single INSERT statement,
        # which may exceed the SQL server's maximum parameter count.
        # For example, if you have 1000 records and a batch size of 100,
        # 10 separate INSERT statements will be generated, each with 100 records.
        statements = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            statement = self._create_single_insert(table_name, fields, batch)
            statements.append(statement)
        return statements

    def _create_single_insert(self, table_name: str, fields: list, records: list) -> str:
        """Create a single INSERT statement for a batch of records"""

        field_names = [f['name'] for f in fields]
        
        values_list = []
        for record in records:
            record_values = []
            for field in fields:
                value = record[field['name']]
                formatted_value = self.format_value(
                    value, 
                    field['type'],
                    field.get('decimal', 0)
                )
                record_values.append(formatted_value)
            values_list.append(f"({','.join(record_values)})")
            
        return f"INSERT INTO {table_name} ({','.join(field_names)}) VALUES {','.join(values_list)}"

 
    def _validate_table_name(self, table_name: str) -> bool:
        """
        Currently only checks if alphanumeric + underscore.
        Should also:
        - Check for SQL keywords
        - Validate length
        - Handle schema names (e.g., 'schema.table')
        """
        # Example: Allow only alphanumeric and underscores
        return table_name.replace("_", "").isalnum()    