"""
Configuration management module for ETL process
Handles loading and parsing configuration files
"""
import os
import logging
import yaml
from sqlalchemy import create_engine, MetaData

logger = logging.getLogger(__name__)

def load_yaml_file(file_path):
    """
    Load a YAML file and return its contents as a dictionary
    
    Args:
        file_path (str): Path to the YAML file
        
    Returns:
        dict: Contents of the YAML file
    """
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error loading YAML file {file_path}: {str(e)}")
        raise

def load_config(config_dir):
    """
    Load all configuration files from the specified directory
    
    Args:
        config_dir (str): Directory containing configuration files
        
    Returns:
        dict: Combined configuration from all files
    """
    config = {
        'source': {},
        'warehouse': {},
        'sql_scripts': {}
    }
    
    try:
        # Load source database configuration
        source_config_path = os.path.join(config_dir, 'source.yml')
        config['source'] = load_yaml_file('config/source_db.yml')
        
        # Load data warehouse configuration
        warehouse_config_path = os.path.join(config_dir, 'warehouse_db.yml')
        config['warehouse'] = load_yaml_file('config/warehouse_db.yml')
        
        # Load SQL scripts
        sql_scripts_path = os.path.join(config_dir, 'sqlscripts.yml')
        config['sql_scripts'] = load_yaml_file(sql_scripts_path)
        
        # Create connection strings
        config['source']['connection_string'] = build_connection_string(config['source'])
        config['warehouse']['connection_string'] = build_connection_string(config['warehouse'])
        
        # Create database engines
        config['source']['engine'] = create_engine(config['source']['connection_string'])
        config['warehouse']['engine'] = create_engine(config['warehouse']['connection_string'])
        
        # Create metadata objects
        config['source']['metadata'] = MetaData()
        config['warehouse']['metadata'] = MetaData()
        
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise

def build_connection_string(db_config):
    """
    Build a SQLAlchemy connection string from database configuration
    
    Args:
        db_config (dict): Database configuration
        
    Returns:
        str: SQLAlchemy connection string
    """
    driver = db_config.get('driver', 'postgresql')
    user = db_config.get('user', '')
    password = db_config.get('password', '')
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', '5432')
    database = db_config.get('database', '')
    
    # Handle different database types
    if driver.lower() in ['postgresql', 'postgres']:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    elif driver.lower() == 'mysql':
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    elif driver.lower() == 'oracle':
        return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}"
    elif driver.lower() in ['mssql', 'sqlserver']:
        return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    else:
        raise ValueError(f"Unsupported database driver: {driver}")