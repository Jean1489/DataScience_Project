#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para cargar datos transformados en el Data Warehouse.
"""

import psycopg2
import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger('ETL_Mensajeria.Load')

class DataLoader:
    """Clase para cargar datos en el Data Warehouse"""
    
    def __init__(self, config, sql_scripts):
        """Inicializa conexión al Data Warehouse"""
        self.config = config
        self.sql_scripts = sql_scripts
        self.connection = self._connect()
        self.engine = create_engine(
            f"postgresql://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
    
    def _connect(self):
        """Establece conexión con el Data Warehouse"""
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            )
            logger.info(f"Conexión exitosa al Data Warehouse: {self.config['database']}")
            return conn
        except Exception as e:
            logger.error(f"Error al conectar al Data Warehouse: {e}")
            raise
    
    def execute_sql(self, sql_statement):
        """Ejecuta una sentencia SQL en el Data Warehouse"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql_statement)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Error al ejecutar SQL: {e}")
            self.connection.rollback()
            raise
    
    def create_warehouse_schema(self):
        """Crea el esquema del Data Warehouse si no existe"""
        try:
            for table_name, sql in self.sql_scripts['create_tables'].items():
                self.execute_sql(sql)
            logger.info("Esquema del Data Warehouse creado/verificado")
        except Exception as e:
            logger.error(f"Error al crear esquema del Data Warehouse: {e}")
            raise
    
    def load_dimension(self, df, dimension):
        """
        Carga datos en una tabla de dimensión
        
        Args:
            df: DataFrame con datos transformados
            dimension: Nombre de la dimensión a cargar
        """
        try:
            # Nombre de la tabla de dimensión
            dim_table = f"dim_{dimension}"
            
            # Cargar DataFrame a tabla temporal
            temp_table = f"{dim_table}_temp"
            df.to_sql(temp_table, self.engine, if_exists='replace', index=False)
            
            # Ejecutar merge mediante SQL para actualización/inserción
            merge_sql = self.sql_scripts['merge_dimensions'].get(dimension)
            
            if merge_sql:
                self.execute_sql(merge_sql)
                logger.info(f"Carga de dimensión {dimension} completada")
            else:
                logger.warning(f"No se encontró script de merge para dimensión {dimension}")
        except Exception as e:
            logger.error(f"Error al cargar dimensión {dimension}: {e}")
            raise
    
    def load_derived_dimensions(self, transformed_data):
        """
        Carga dimensiones derivadas (tiempo, geografía, tiempo_espera)
        
        Args:
            transformed_data: Dict con DataFrames transformados
        """
        try:
            # Cargar dimensión tiempo
            df_tiempo = transformed_data['tiempo']
            df_tiempo.to_sql('dim_tiempo_temp', self.engine, if_exists='replace', index=False)
            self.execute_sql(self.sql_scripts['merge_dimensions']['tiempo'])
            logger.info("Carga de dimensión tiempo completada")
            
            # Cargar dimensión geografía
            df_geografia = transformed_data['geografia']
            df_geografia.to_sql('dim_geografia_temp', self.engine, if_exists='replace', index=False)
            self.execute_sql(self.sql_scripts['merge_dimensions']['geografia'])
            logger.info("Carga de dimensión geografía completada")
            
            # Cargar dimensión tiempo_espera
            df_tiempo_espera = transformed_data['tiempo_espera']
            df_tiempo_espera.to_sql('dim_tiempo_espera_temp', self.engine, if_exists='replace', index=False)
            self.execute_sql(self.sql_scripts['merge_dimensions']['tiempo_espera'])
            logger.info("Carga de dimensión tiempo_espera completada")
        except Exception as e:
            logger.error(f"Error al cargar dimensiones derivadas: {e}")
            raise
    
    def load_fact_table(self, transformed_data):
        """
        Carga datos en la tabla de hechos
        
        Args:
            transformed_data: Dict con DataFrames transformados
        """
        try:
            # Obtener IDs para cada dimensión
            df_fact = transformed_data['servicios'].copy()
            
            # Obtener mapeos desde dimensiones
            mappings = self._get_dimension_mappings()
            
            # Aplicar mapeos para obtener IDs de dimensiones
            df_fact = self._apply_dimension_mappings(df_fact, mappings, transformed_data)
            
            # Seleccionar columnas para la tabla de hechos
            fact_columns = [
                'id_servicio', 'id_cliente', 'id_sede', 'id_mensajero', 'id_tiempo', 
                'id_geografia', 'id_estado', 'id_novedad', 'id_tiempo_espera', 
                'id_tipo_servicio', 'id_medio_pago', 'tiempo_total_servicio', 'tiempo_por_fase', 
                'tiempo_espera', 'costo_servicio', 'descuentos_aplicados'
            ]
            
            # Asegurar que todas las columnas existan
            for col in fact_columns:
                if col not in df_fact.columns:
                    df_fact[col] = None
            
            df_fact_final = df_fact[fact_columns]
            
            # Cargar en tabla temporal
            df_fact_final.to_sql('fact_servicios_mensajeria_temp', self.engine, if_exists='replace', index=False)
            
            # Ejecutar merge para actualización de tabla de hechos
            self.execute_sql(self.sql_scripts['load_fact_table'])
            
            logger.info(f"Carga de tabla de hechos completada, {len(df_fact_final)} registros procesados")
        except Exception as e:
            logger.error(f"Error al cargar tabla de hechos: {e}")
            raise
    
    def _get_dimension_mappings(self):
        """Obtiene mapeos de claves primarias desde dimensiones"""
        mappings = {}
        
        # Dimensiones estándar
        dimensions = [
            ('cliente', 'id_cliente', 'nombre_cliente'),
            ('sede', 'id_sede', 'nombre_sede'),
            ('mensajero', 'id_mensajero', 'nombre_mensajero'),
            ('estado', 'id_estado', 'estado_servicio'),
            ('novedad', 'id_novedad', 'descripcion_novedad'),
            ('tipo_servicio', 'id_tipo_servicio', 'tipo_servicio'),
            ('medio_pago', 'id_medio_pago', 'metodo_pago')
        ]
        
        for dim_name, id_col, value_col in dimensions:
            query = f"SELECT {id_col}, {value_col} FROM dim_{dim_name}"
            mappings[dim_name] = pd.read_sql(query, self.connection)
        
        # Dimensiones derivadas
        mappings['tiempo'] = pd.read_sql(
            "SELECT id_tiempo, fecha, hora FROM dim_tiempo", 
            self.connection
        )
        mappings['tiempo']['fecha'] = pd.to_datetime(mappings['tiempo']['fecha']).dt.date
        
        mappings['geografia'] = pd.read_sql(
            "SELECT id_geografia, ciudad_origen, ciudad_destino FROM dim_geografia", 
            self.connection
        )
        
        mappings['tiempo_espera'] = pd.read_sql(
            "SELECT id_tiempo_espera, categoria, rango_tiempo FROM dim_tiempo_espera", 
            self.connection
        )
        
        return mappings
    
    def _apply_dimension_mappings(self, df, mappings, transformed_data):
        """Aplica mapeos para obtener IDs de dimensiones"""
        # Dimensiones estándar
        df = df.merge(mappings['cliente'], on='id_cliente', how='left')
        df = df.merge(mappings['sede'], on='id_sede', how='left')
        df = df.merge(mappings['mensajero'], on='id_mensajero', how='left')
        df = df.merge(mappings['estado'], left_on='estado_servicio', right_on='estado_servicio', how='left')
        df = df.merge(mappings['novedad'], on='id_novedad', how='left')
        df = df.merge(mappings['tipo_servicio'], left_on='tipo_servicio', right_on='tipo_servicio', how='left')
        df = df.merge(mappings['medio_pago'], left_on='medio_pago', right_on='metodo_pago', how='left')
        
        # Dimensión tiempo
        df['fecha'] = pd.to_datetime(df['fecha_hora_servicio']).dt.date
        df['hora'] = pd.to_datetime(df['fecha_hora_servicio']).dt.hour
        df = df.merge(mappings['tiempo'], on=['fecha', 'hora'], how='left')
        
        # Dimensión geografía
        df = df.merge(mappings['geografia'], on=['ciudad_origen', 'ciudad_destino'], how='left')
        
        # Dimensión tiempo_espera
        df = df.merge(mappings['tiempo_espera'], left_on=['categoria_espera', 'rango_tiempo'], 
                     right_on=['categoria', 'rango_tiempo'], how='left')
        
        return df
    
    def close_connection(self):
        """Cierra la conexión al Data Warehouse"""
        if self.connection:
            self.connection.close()
            logger.info("Conexión al Data Warehouse cerrada")