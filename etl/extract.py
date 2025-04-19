#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para extraer datos de la base de datos origen.
"""

import psycopg2
import pandas as pd
import logging

logger = logging.getLogger('ETL_Mensajeria.Extract')

class DataExtractor:
    """Clase para extraer datos de la base de datos origen"""
    
    def __init__(self, config):
        """Inicializa conexión a la base de datos origen"""
        self.config = config
        self.connection = self._connect()
        self.dimension_queries = {
            'cliente': """
                SELECT 
                    id_cliente, 
                    nombre_cliente, 
                    tipo_cliente, 
                    industria
                FROM clientes
            """,
            'sede': """
                SELECT 
                    id_sede, 
                    nombre_sede, 
                    ciudad, 
                    direccion
                FROM sedes
            """,
            'mensajero': """
                SELECT 
                    id_mensajero, 
                    nombre as nombre_mensajero, 
                    antiguedad, 
                    puntuacion as puntuacion_servicio
                FROM mensajeros
            """,
            'estado': """
                SELECT 
                    id_estado, 
                    estado as estado_servicio
                FROM estados
            """,
            'novedad': """
                SELECT 
                    id_novedad, 
                    descripcion as descripcion_novedad, 
                    tipo_novedad as tipo
                FROM novedades
            """,
            'tipo_servicio': """
                SELECT 
                    id_tipo as id_tipo_servicio, 
                    tipo as tipo_servicio
                FROM tipos_servicio
            """,
            'medio_pago': """
                SELECT 
                    id_medio as id_medio_pago, 
                    metodo as metodo_pago
                FROM medios_pago
            """
        }
        self.services_query = """
            SELECT 
                s.id_servicio, 
                s.id_cliente, 
                s.id_sede, 
                s.id_mensajero,
                s.fecha_hora_servicio,
                s.ciudad_origen,
                s.ciudad_destino,
                s.estado_servicio,
                s.id_novedad,
                s.tiempo_espera,
                s.tipo_servicio,
                s.medio_pago,
                s.tiempo_total,
                s.tiempo_fase1,
                s.tiempo_fase2,
                s.tiempo_fase3,
                s.costo_servicio,
                s.descuento
            FROM 
                servicios s
        """
    
    def _connect(self):
        """Establece conexión con la base de datos origen"""
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            )
            logger.info(f"Conexión exitosa a la base de datos origen: {self.config['database']}")
            return conn
        except Exception as e:
            logger.error(f"Error al conectar a la base de datos origen: {e}")
            raise
    
    def extract_dimension(self, dimension):
        """Extrae datos para una dimensión específica"""
        try:
            if dimension not in self.dimension_queries:
                raise ValueError(f"No existe consulta definida para dimensión: {dimension}")
            
            query = self.dimension_queries[dimension]
            df = pd.read_sql(query, self.connection)
            logger.info(f"Extracción de dimensión {dimension} completada: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Error al extraer dimensión {dimension}: {e}")
            raise
    
    def extract_services(self):
        """Extrae datos de servicios para la tabla de hechos"""
        try:
            df = pd.read_sql(self.services_query, self.connection)
            logger.info(f"Extracción de servicios completada: {len(df)} registros")
            return df
        except Exception as e:
            logger.error(f"Error al extraer servicios: {e}")
            raise
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.connection:
            self.connection.close()
            logger.info("Conexión a base de datos origen cerrada")