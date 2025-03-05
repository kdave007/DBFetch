import os
import configparser
from pathlib import Path
import sys
from typing import Any, Dict, Optional, List
import json

DEFAULT_CONFIG = {
   'paths': {
        'dbf_directory': '../mockDBF/CANCFFI.DBF',
        'sql_output': '../'
    },
    'features': {
        'execute_query': 'false',
        'preview_mode': 'false',
        'preview_ext': 'txt',
        'max_records': '4000',
        'target_table_name_active': 'false',
        'target_table_name': 'test_table'
    },
    'database': {
        'host': 'localhost',
        'port': '5432',
        'database': 'postgres',
        'user': 'postgres',
        'password': 'default_password'
    }   
}

DEFAULT_FILTER_CONFIG = {
    "filters": [],
    "filter_enabled": False,
    "max_records": 1000
}

# Configuration validation rules
CONFIG_RULES = {
    'preview_ext': ['txt', 'sql'],
    'max_records': (1, 10000),  # (min, max) for numeric values
    'port': (1, 65535)  # Valid port range
}

def validate_config_value(key: str, value: Any) -> Any:
    """
    Validate configuration values against rules.
    
    Args:
        key: Configuration key to validate
        value: Value to validate
        
    Returns:
        Validated value or default if invalid
    """
    if key in CONFIG_RULES:
        if isinstance(CONFIG_RULES[key], list):
            if value not in CONFIG_RULES[key]:
                print(f"Warning: Invalid {key} value '{value}'. Using default: {DEFAULT_CONFIG['features'].get(key)}")
                return DEFAULT_CONFIG['features'].get(key)
        elif isinstance(CONFIG_RULES[key], tuple) and len(CONFIG_RULES[key]) == 2:
            try:
                num_value = int(value)
                min_val, max_val = CONFIG_RULES[key]
                if not (min_val <= num_value <= max_val):
                    print(f"Warning: {key} value {value} out of range ({min_val}-{max_val}). Using default.")
                    return DEFAULT_CONFIG['features'].get(key)
            except ValueError:
                print(f"Warning: Invalid numeric value for {key}: {value}")
                return DEFAULT_CONFIG['features'].get(key)
    return value

def load_config(config_path: str) -> configparser.ConfigParser:
    """
    Load configuration from a file with defaults.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        ConfigParser instance with loaded configuration
    """
    config = configparser.ConfigParser()
    
    # First load defaults
    config.read_dict(DEFAULT_CONFIG)
    
    # Then try to load user config
    if os.path.exists(config_path):
        try:
            config.read(config_path, encoding='utf-8')
        except Exception as e:
            print(f"Warning: Error reading config file {config_path}: {e}")
            print("Using default configuration")
    else:
        print(f"Warning: Config file not found at {config_path}")
        print("Using default configuration")
    
    return config

def get_app_path() -> Path:
    """
    Get the application base path that works both in dev and PyInstaller.
    
    Returns:
        Path to application directory
    """
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent
    else:
        return Path(__file__).resolve().parent

# Initialize configuration
BASE_DIR = get_app_path()
CONFIG = load_config(os.path.join(BASE_DIR, 'config.ini'))
SECURE_CONFIG_PATH = os.getenv('DBFORGE_SECURE_CONFIG', os.path.join(BASE_DIR, 'secure', 'secure_config.ini'))
SECURE_CONFIG = load_config(SECURE_CONFIG_PATH)

def get_config_value(section: str, key: str, default: Any = None, as_boolean: bool = False, as_int: bool = False) -> Any:
    """
    Get configuration value with validation and type conversion.
    
    Args:
        section: Configuration section name
        key: Configuration key
        default: Default value if not found
        as_boolean: Whether to convert value to boolean
        as_int: Whether to convert value to integer
    """
    try:
        if as_int:
            value = CONFIG[section].getint(key, default)
            return validate_config_value(key, value)
        if as_boolean:
            return CONFIG[section].getboolean(key, default)
        value = CONFIG[section].get(key, default)
        return validate_config_value(key, value)
    except Exception as e:
        print(f"Warning: Error getting config value [{section}]{key}: {e}")
        return default

# Path configuration
PATH_CONFIG = {
    'dbf_directory': get_config_value('paths', 'dbf_directory', DEFAULT_CONFIG['paths']['dbf_directory']),
    'sql_output': get_config_value('paths', 'sql_output', DEFAULT_CONFIG['paths']['sql_output'])
}

# Feature flags
FEATURE_FLAGS = {
    'preview_mode': get_config_value('features', 'preview_mode', False, as_boolean=True),
    'preview_ext': get_config_value('features', 'preview_ext', DEFAULT_CONFIG['features']['preview_ext']),
    'execute_query': get_config_value('features', 'execute_query', False, as_boolean=True),
    'max_records': get_config_value('features', 'max_records', DEFAULT_CONFIG['features']['max_records'], as_int=True),
    'target_table_name_active': get_config_value('features', 'target_table_name_active', False, as_boolean=True),
    'target_table_name': get_config_value('features', 'target_table_name', DEFAULT_CONFIG['features']['target_table_name'])
}

# Database configuration
DB_CONFIG = {
    'host': SECURE_CONFIG['database']['host'],
    'port': SECURE_CONFIG['database'].getint('port', int(DEFAULT_CONFIG['database']['port'])),
    'database': SECURE_CONFIG['database']['database'],
    'user': SECURE_CONFIG['database']['user'],
    'password': SECURE_CONFIG['database']['password']
}

# Path getters
def get_dbf_directory() -> str:
    """Get DBF directory path"""
    return PATH_CONFIG['dbf_directory']

def get_sql_output_directory() -> str:
    """Get SQL output directory path"""
    return PATH_CONFIG['sql_output']

# Feature flag getters
def get_preview_mode() -> bool:
    """Get preview mode setting"""
    return FEATURE_FLAGS['preview_mode']

def get_preview_extension() -> str:
    """Get preview file extension"""
    return FEATURE_FLAGS['preview_ext']

def is_execute_query_enabled() -> bool:
    """Get execute query setting"""
    return FEATURE_FLAGS['execute_query']

def get_max_records() -> int:
    """Get maximum records limit"""
    return FEATURE_FLAGS['max_records']

def is_custom_table_name_enabled() -> bool:
    """Get whether custom table naming is enabled"""
    return FEATURE_FLAGS['target_table_name_active']

def get_target_table_name() -> str:
    """Get target table name"""
    return FEATURE_FLAGS['target_table_name']

def load_filter_config(filter_config_path: Optional[str] = None) -> Dict:
    """
    Load filter configuration from JSON file.
    
    Args:
        filter_config_path: Path to filter config JSON file. If None, uses default path.
        
    Returns:
        Dict containing filter configuration
    """
    if filter_config_path is None:
        filter_config_path = os.path.join(BASE_DIR, 'filter_config.json')
    
    try:
        if os.path.exists(filter_config_path):
            with open(filter_config_path, 'r') as f:
                config = json.load(f)
                # Validate required fields
                if not isinstance(config.get('filters', []), list):
                    raise ValueError("'filters' must be a list")
                return config
    except Exception as e:
        print(f"Warning: Error reading filter config file {filter_config_path}: {e}")
        print("Using default filter configuration")
    
    return DEFAULT_FILTER_CONFIG

def validate_filter_conditions(conditions: List[Dict]) -> List[Dict]:
    """
    Validate filter conditions from config.
    
    Args:
        conditions: List of filter condition dictionaries
        
    Returns:
        List of validated filter conditions
    """
    valid_conditions = []
    valid_operators = ['=', '>', '<', '>=', '<=', 'between']
    
    for condition in conditions:
        try:
            # Check required fields
            if 'field' not in condition or 'value' not in condition:
                print(f"Warning: Skipping invalid condition (missing required fields): {condition}")
                continue
            
            # Validate operator
            operator = condition.get('operator', '=')
            if operator not in valid_operators:
                print(f"Warning: Invalid operator '{operator}' in condition: {condition}")
                continue
                
            # Check for value2 if operator is 'between'
            if operator == 'between' and 'value2' not in condition:
                print(f"Warning: Missing 'value2' for 'between' operator in condition: {condition}")
                continue
                
            valid_conditions.append(condition)
        except Exception as e:
            print(f"Warning: Error validating condition: {e}")
            continue
    
    return valid_conditions

# Load filter configuration
FILTER_CONFIG = load_filter_config()

def get_filter_conditions() -> List[Dict]:
    """Get validated filter conditions from config"""
    if not FILTER_CONFIG.get('filter_enabled', False):
        return []
    return validate_filter_conditions(FILTER_CONFIG.get('filters', []))

def is_filtering_enabled() -> bool:
    """Check if filtering is enabled in config"""
    return FILTER_CONFIG.get('filter_enabled', False)
