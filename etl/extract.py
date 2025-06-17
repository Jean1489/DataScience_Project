"""
Data extraction module for ETL process
Extracts data from source systems based on configuration
"""
import logging
import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def extract_data(config):
    """
    Extract data from source systems
    
    Args:
        config (dict): ETL configuration
        
    Returns:
        dict: Dictionary containing extracted dataframes
    """
    logger.info("Starting data extraction")
    extracted_data = {}
    
    try:
        source_engine = config['source']['engine']
        warehouse_engine = config['warehouse']['engine']
        
        # Extract data for each table defined in SQL scripts
        for table_name, query in config['sql_scripts']['extract'].items():
            logger.info(f"Extracting data for {table_name}")
            
            # Apply date parameters if needed
            query = apply_date_parameters(query)
            
            # Execute the query and load into a DataFrame
            with source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
                
            logger.info(f"Extracted {len(df)} rows for {table_name}")
            extracted_data[table_name] = df
        
        return extracted_data
    
    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}", exc_info=True)
        raise

def apply_date_parameters(query):
    """
    Apply date parameters to a query
    
    Args:
        query (str): SQL query with date parameters
        
    Returns:
        str: SQL query with date parameters replaced
    """
    # Get current date and time
    now = datetime.now()
    
    # Define common date parameters
    date_params = {
        "{CURRENT_DATE}": now.strftime("%Y-%m-%d"),
        "{YESTERDAY}": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
        "{CURRENT_MONTH_START}": now.replace(day=1).strftime("%Y-%m-%d"),
        "{PREVIOUS_MONTH_START}": (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d"),
        "{PREVIOUS_MONTH_END}": (now.replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d"),
        "{CURRENT_YEAR}": str(now.year),
        "{PREVIOUS_YEAR}": str(now.year - 1)
    }
    
    # Replace parameters in query
    for param, value in date_params.items():
        query = query.replace(param, value)
    
    return query

def extract_incremental_data(source_engine, table_name, id_column, last_loaded_id):
    """
    Extract data incrementally based on ID
    
    Args:
        source_engine: SQLAlchemy engine for source database
        table_name (str): Source table name
        id_column (str): ID column name
        last_loaded_id: Last loaded ID value
        
    Returns:
        DataFrame: Extracted data
    """
    query = f"""
    SELECT * FROM {table_name}
    WHERE {id_column} > {last_loaded_id}
    ORDER BY {id_column}
    """
    
    with source_engine.connect() as conn:
        df = pd.read_sql(text(query), conn)
    
    return df

def get_last_loaded_id(warehouse_engine, tracking_table, source_table):
    """
    Get the last loaded ID for a specific source table
    
    Args:
        warehouse_engine: SQLAlchemy engine for warehouse database
        tracking_table (str): Table tracking ETL loads
        source_table (str): Source table name
        
    Returns:
        int: Last loaded ID
    """
    query = f"""
    SELECT MAX(last_id) AS last_id 
    FROM {tracking_table}
    WHERE source_table = '{source_table}'
    """
    
    with warehouse_engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
        
    return result.last_id if result and result.last_id else 0