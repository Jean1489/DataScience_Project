#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para transformar los datos extraídos y prepararlos para su carga.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger('ETL_Mensajeria.Transform')

class DataTransformer:
    """Clase para transformar datos extraídos"""
    
    def __init__(self):
        """Inicialización del transformador"""
        logger.info("Inicializando transformador de datos")
    
    def transform_dimension(self, df, dimension):
        """
        Transforma datos para una dimensión específica
        
        Args:
            df: DataFrame con datos extraídos
            dimension: Nombre de la dimensión a transformar
            
        Returns:
            DataFrame con datos transformados
        """
        logger.info(f"Transformando dimensión: {dimension}")
        
        # Limpiar valores nulos
        df = df.fillna({
            'nombre_cliente': 'Sin nombre',
            'tipo_cliente': 'No especificado',
            'industria': 'No especificada',
            'nombre_sede': 'Sede sin nombre',
            'ciudad': 'Sin ciudad',
            'direccion': 'Sin dirección',
            'nombre_mensajero': 'Sin nombre',
            'estado_servicio': 'Sin estado',
            'descripcion_novedad': 'Sin descripción',
            'tipo': 'No especificado',
            'tipo_servicio': 'No especificado',
            'metodo_pago': 'No especificado'
        })
        
        # Transformaciones específicas por dimensión
        if dimension == 'mensajero':
            # Asegurar que antiguedad sea un entero
            df['antiguedad'] = df['antiguedad'].fillna(0).astype(int)
            # Asegurar que puntuación esté en escala de 0-5
            df['puntuacion_servicio'] = df['puntuacion_servicio'].fillna(0)
            df['puntuacion_servicio'] = df['puntuacion_servicio'].apply(
                lambda x: max(0, min(5, x))  # Limitar entre 0 y 5
            )
        
        logger.info(f"Transformación de dimensión {dimension} completada")
        return df
    
    def transform_services(self, df):
        """
        Transforma datos de servicios y genera dimensiones derivadas
        
        Args:
            df: DataFrame con datos de servicios extraídos
            
        Returns:
            Dict con DataFrames transformados para cada componente
        """
        logger.info("Transformando datos de servicios")
        
        result = {
            'servicios': df.copy()
        }
        
        # Procesar dimensión tiempo
        df_tiempo = self._transform_tiempo(df)
        result['tiempo'] = df_tiempo
        
        # Procesar dimensión geografía
        df_geografia = self._transform_geografia(df)
        result['geografia'] = df_geografia
        
        # Procesar dimensión tiempo_espera
        df_tiempo_espera = self._transform_tiempo_espera(df)
        result['tiempo_espera'] = df_tiempo_espera
        
        # Agregar campos calculados a servicios
        result['servicios']['tiempo_por_fase'] = (
            df['tiempo_fase1'].fillna(0) + 
            df['tiempo_fase2'].fillna(0) + 
            df['tiempo_fase3'].fillna(0)
        )
        
        # Limpiar valores nulos en métricas
        result['servicios']['tiempo_total'] = result['servicios']['tiempo_total'].fillna(0)
        result['servicios']['tiempo_por_fase'] = result['servicios']['tiempo_por_fase'].fillna(0)
        result['servicios']['tiempo_espera'] = result['servicios']['tiempo_espera'].fillna(0)
        result['servicios']['costo_servicio'] = result['servicios']['costo_servicio'].fillna(0)
        result['servicios']['descuento'] = result['servicios']['descuento'].fillna(0)
        
        # Renombrar columnas para consistencia con la estructura del warehouse
        result['servicios'] = result['servicios'].rename(columns={
            'tiempo_total': 'tiempo_total_servicio',
            'descuento': 'descuentos_aplicados'
        })
        
        logger.info("Transformación de servicios completada")
        return result
    
    def _transform_tiempo(self, df):
        """Transforma y extrae la dimensión tiempo"""
        # Convertir a datetime si es necesario
        df['fecha_hora_servicio'] = pd.to_datetime(df['fecha_hora_servicio'])
        
        # Extraer componentes de fecha/hora
        df_tiempo = pd.DataFrame({
            'fecha': df['fecha_hora_servicio'].dt.date,
            'anio': df['fecha_hora_servicio'].dt.year,
            'mes': df['fecha_hora_servicio'].dt.month,
            'dia': df['fecha_hora_servicio'].dt.day,
            'dia_semana': df['fecha_hora_servicio'].dt.dayofweek,
            'hora': df['fecha_hora_servicio'].dt.hour
        })
        
        # Eliminar duplicados
        df_tiempo = df_tiempo.drop_duplicates()
        
        return df_tiempo
    
    def _transform_geografia(self, df):
        """Transforma y extrae la dimensión geografía"""
        # Extraer pares origen-destino únicos
        df_geografia = df[['ciudad_origen', 'ciudad_destino']].drop_duplicates()
        
        # Generar distancia aproximada (simulada)
        # En un caso real, se podría usar una API de geocodificación o datos reales
        np.random.seed(42)  # Para reproducibilidad
        df_geografia['distancia_aproximada'] = np.random.randint(5, 500, size=len(df_geografia))
        
        return df_geografia
    
    def _transform_tiempo_espera(self, df):
        """Transforma y extrae la dimensión tiempo_espera"""
        # Función para categorizar tiempos
        def categorizar_tiempo(tiempo):
            if pd.isna(tiempo) or tiempo < 5:
                return 'Muy Rápido', '0-5 minutos'
            elif tiempo < 15:
                return 'Rápido', '5-15 minutos'
            elif tiempo < 30:
                return 'Normal', '15-30 minutos'
            elif tiempo < 60:
                return 'Lento', '30-60 minutos'
            else:
                return 'Muy Lento', '60+ minutos'
        
        # Crear DataFrame temporal con categorías
        categorias = []
        for i, row in df.iterrows():
            categoria, rango = categorizar_tiempo(row['tiempo_espera'])
            categorias.append({'categoria': categoria, 'rango_tiempo': rango})
        
        df_cat = pd.DataFrame(categorias)
        
        # Crear DataFrame de dimensión con valores únicos
        df_tiempo_espera = df_cat[['categoria', 'rango_tiempo']].drop_duplicates()
        
        # Agregar categorías a DataFrame original para usar en mapeo posterior
        df['categoria_espera'] = df_cat['categoria']
        df['rango_tiempo'] = df_cat['rango_tiempo']
        
        return df_tiempo_espera