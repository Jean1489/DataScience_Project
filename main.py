#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Proceso ETL principal para Data Warehouse de Servicios de Mensajería.
Este script orquesta el flujo completo de ETL conectando las operaciones
de extracción, transformación y carga.
"""

import os
import logging
import yaml
import datetime
from etl.extract import DataExtractor
from etl.transform import DataTransformer
from etl.load import DataLoader

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='etl_mensajeria.log'
)
logger = logging.getLogger('ETL_Mensajeria')

def load_config(config_file):
    """Carga configuración desde archivo YAML"""
    try:
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error al cargar configuración desde {config_file}: {e}")
        raise

def load_sql_scripts(sql_file):
    """Carga scripts SQL desde archivo YAML"""
    try:
        with open(sql_file, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error al cargar scripts SQL desde {sql_file}: {e}")
        raise

def main():
    """Función principal que ejecuta el proceso ETL completo"""
    start_time = datetime.datetime.now()
    logger.info(f"Iniciando proceso ETL: {start_time}")
    
    try:
        # Cargar configuraciones
        source_config = load_config('config/source_db.yml')
        warehouse_config = load_config('config/warehouse_db.yml')
        sql_scripts = load_sql_scripts('config/sqlscripts.yml')
        
        # Inicializar componentes ETL
        extractor = DataExtractor(source_config)
        transformer = DataTransformer()
        loader = DataLoader(warehouse_config, sql_scripts)
        
        # Crear esquema del warehouse si no existe
        loader.create_warehouse_schema()
        
        # ETL para dimensiones
        dimensions = [
            'cliente', 'sede', 'mensajero', 'estado', 
            'novedad', 'tipo_servicio', 'medio_pago'
        ]
        
        for dim in dimensions:
            # Extraer datos para dimensión
            raw_data = extractor.extract_dimension(dim)
            
            # Transformar datos
            transformed_data = transformer.transform_dimension(raw_data, dim)
            
            # Cargar dimensión
            loader.load_dimension(transformed_data, dim)
        
        # ETL para tabla de hechos
        # 1. Extraer datos de servicios
        raw_services = extractor.extract_services()
        
        # 2. Transformar datos y generar dimensiones derivadas
        transformed_data = transformer.transform_services(raw_services)
        
        # 3. Cargar dimensiones derivadas (tiempo, geografía, tiempo_espera)
        loader.load_derived_dimensions(transformed_data)
        
        # 4. Cargar tabla de hechos
        loader.load_fact_table(transformed_data)
        
        # Cerrar conexiones
        extractor.close_connection()
        loader.close_connection()
        
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logger.info(f"Proceso ETL completado. Duración: {duration}")
        print(f"ETL ejecutado correctamente. Ver detalles en el log 'etl_mensajeria.log'")
        
    except Exception as e:
        logger.error(f"Error en proceso ETL: {e}")
        print(f"Error durante ETL: {e}")

if __name__ == "__main__":
    main()