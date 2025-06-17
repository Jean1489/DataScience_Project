"""
ETL orchestrator for the Messaging Services Data Warehouse
This script orchestrates the ETL process, running extraction, transformation and loading
"""
import logging
import os
import sys
from datetime import datetime

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.config import load_config

# Crear directorio para logs si no existe
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

# Definir ruta del archivo de log
log_filename = os.path.join(log_dir, f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_etl(config_dir="config"):
    """
    Main ETL process that orchestrates extraction, transformation and loading
    """
    try:
        start_time = datetime.now()
        logger.info(f"Starting ETL process at {start_time}")
        
        # Load configuration
        logger.info("Loading configuration files")
        config = load_config(config_dir)
        
        # Extract data from source systems
        logger.info("Starting data extraction")
        extracted_data = extract_data(config)
        logger.info(f"Extraction completed. Extracted {len(extracted_data)} datasets")
        
        # Transform the extracted data
        logger.info("Starting data transformation")
        transformed_data = transform_data(extracted_data, config)
        logger.info("Transformation completed")
        
        # Load data into the data warehouse
        logger.info("Starting data loading to warehouse")
        load_results = load_data(transformed_data, config)
        logger.info("Data loading completed")
        
        # Log completion statistics
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"ETL process completed successfully at {end_time}")
        logger.info(f"Total duration: {duration}")
        logger.info(f"Loaded {load_results['rows_loaded']} rows across {load_results['tables_loaded']} tables")
        
        return True
    
    except Exception as e:
        logger.error(f"ETL process failed with error: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    success = run_etl()
    sys.exit(0 if success else 1)