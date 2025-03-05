# DBFetch

A powerful Python tool for converting DBF files to SQL, with advanced filtering capabilities and database integration.

## Features

- **DBF File Processing**: Read and process DBF files efficiently
- **SQL Generation**: Convert DBF records to SQL statements
- **Advanced Filtering**:
  - Date range filtering using 'between' operator
  - Multiple conditions with AND logic
  - Various comparison operators (=, >, <, >=, <=, between)
  - Date type handling for both date and datetime fields
- **Preview Mode**: Generate SQL files for review before execution
- **Direct Database Execution**: Optional feature to execute SQL statements directly
- **Batch Processing**: Process large files in configurable batch sizes
- **Comprehensive Logging**: Detailed logs for monitoring and debugging
- **Secure Configuration**: Separate secure configuration for sensitive database credentials

## Configuration

### config.ini
```ini
[paths]
dbf_directory = path/to/your/dbf/files
sql_output = path/to/output/directory

[features]
execute_query = true/false
preview_mode = true/false
preview_ext = txt/sql
max_records = 4000
```

### secure/secure_config.ini
This file contains sensitive database credentials and should never be committed to version control.

```ini
[database]
host = your_database_host
port = 5432
database = your_database_name
user = your_database_user
password = your_database_password
```

You can also set the secure config path using an environment variable:
```bash
DBFORGE_SECURE_CONFIG=/path/to/your/secure_config.ini
```

### Filter Configuration Example
```json
{
  "filter_enabled": true,
  "filters": [
    {
      "field": "DATE_FIELD",
      "value": "2023-09-18",
      "value2": "2023-09-20",
      "operator": "between"
    },
    {
      "field": "STATUS",
      "value": "I",
      "operator": "="
    }
  ]
}
```

## Requirements

- Python 3.x
- Required packages (see requirements.txt):
  - dbfread>=2.0.7
  - cryptography>=41.0.0
  - python-dotenv>=1.0.0
  - pytest>=7.4.0

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/DBFetch.git
cd DBFetch
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your settings:
   - Copy `config.example.ini` to `config.ini` and modify settings
   - Create `secure/secure_config.ini` with your database credentials
   - Or set `DBFORGE_SECURE_CONFIG` environment variable to point to your secure config

## Usage

1. Basic usage:
```bash
python main.py
```

2. The application will:
   - Read DBF files from the configured directory
   - Apply any configured filters
   - Generate SQL statements
   - Save preview files if enabled
   - Execute SQL if enabled

## Logging

Logs are stored in the `logs` directory with the filename format `dbforge_YYYYMMDD.log`. Logs include:
- Application lifecycle events
- Processing progress
- SQL execution status
- Errors and exceptions

## Security Best Practices

1. Never commit `secure_config.ini` to version control
2. Add `secure/` to your `.gitignore` file
3. Use environment variables in production environments
4. Regularly rotate database credentials
5. Use minimum required database privileges

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

