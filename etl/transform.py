"""
Data transformation module for ETL process
Transforms extracted data into the format required by the data warehouse
"""
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text

logger = logging.getLogger(__name__)

def transform_data(extracted_data, config):
    """
    Transform extracted data for loading into the data warehouse
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        config (dict): ETL configuration
        
    Returns:
        dict: Dictionary of transformed dataframes
    """
    logger.info("Starting data transformation")
    transformed_data = {
        'dimensions': {},
        'facts': {}
    }
    
    try:
        # Transform dimension tables
        transform_dimensions(extracted_data, transformed_data, config)
        
        # Transform fact tables
        transform_facts(extracted_data, transformed_data, config)
        
        return transformed_data
    
    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}", exc_info=True)
        raise

def transform_dimensions(extracted_data, transformed_data, config):
    """
    Transform dimension tables
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        transformed_data (dict): Dictionary to store transformed dataframes
        config (dict): ETL configuration
    """
    # Process dimension transformations based on configuration
    dim_transforms = config['sql_scripts'].get('transform_dimensions', {})
    
    # List of dimension tables from the data warehouse design
    dimension_tables = [
        'dimarea', 'dimtipocliente', 'dimciudad',
        'dimcliente', 'dimsede', 'dimmensajero', 
        'dimtiempo', 'dimusuario', 'dimdireccion', 
        'dimtipopago', 'dimtipovehiculo', 'dimtiponovedad'
    ]
    
    for dim_table in dimension_tables:
        if dim_table in dim_transforms:
            logger.info(f"Transforming {dim_table}")
            transform_function = globals().get(f"transform_{dim_table.lower()}")
            
            if transform_function:
                # Use specific transformation function if available
                transformed_data['dimensions'][dim_table] = transform_function(extracted_data, config)
            else:
                # Use generic transformation based on SQL
                transformed_data['dimensions'][dim_table] = transform_generic_dimension(
                    extracted_data, dim_table, dim_transforms[dim_table], config
                )
        else:
            logger.warning(f"No transformation defined for {dim_table}")

def transform_facts(extracted_data, transformed_data, config):
    """
    Transform fact tables
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        transformed_data (dict): Dictionary to store transformed dataframes
        config (dict): ETL configuration
    """
    # Process fact transformations based on configuration
    fact_transforms = config['sql_scripts'].get('transform_facts', {})
    
    # List of fact tables from the data warehouse design
    fact_tables = [
        'fact_servicios', 'fact_novedades',
        'fact_estados_servicio'
    ]
    
    for fact_table in fact_tables:
        if fact_table in fact_transforms:
            logger.info(f"Transforming {fact_table}")
            transform_function = globals().get(f"transform_{fact_table.lower()}")
            
            if transform_function:
                # Use specific transformation function if available
                transformed_data['facts'][fact_table] = transform_function(extracted_data, transformed_data, config)
            else:
                if fact_table == 'fact_servicios':
                    logger.info("Using specific transformation for fact_servicios")
                    transformed_data['facts'][fact_table] = transform_fact_servicios(
                        extracted_data, transformed_data, config
                    )
                else:
                    # Use generic transformation based on SQL
                    transformed_data['facts'][fact_table] = transform_generic_fact(
                        extracted_data, transformed_data, fact_table, fact_transforms[fact_table], config
                    )
        else:
            logger.warning(f"No transformation defined for {fact_table}")

def transform_generic_dimension(extracted_data, table_name, transform_config, config):
    """
    Apply generic dimension transformation based on configuration
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        table_name (str): Name of the dimension table
        transform_config (dict): Transformation configuration
        config (dict): ETL configuration
        
    Returns:
        DataFrame: Transformed dimension data
    """
    # Get source tables needed for this dimension
    source_tables = transform_config.get('source_tables', [])
    
    # Check if we have all required source data
    for source_table in source_tables:
        if source_table not in extracted_data:
            logger.error(f"Missing required source data: {source_table}")
            raise ValueError(f"Missing required source data: {source_table}")
    
    # Apply transformation SQL if specified
    if 'sql' in transform_config:
        # Create temporary tables for each source dataframe
        with config['source']['engine'].connect() as conn:
            # Log the available tables for debugging
            logger.info(f"Available source tables: {list(extracted_data.keys())}")
            
            # Create temporary tables with consistent naming
            for source_table in source_tables:
                temp_table_name = f"temp_{source_table}"
                logger.info(f"Creating temporary table: {temp_table_name}")
                
                # Create the temporary table - drop if already exists
                try:
                    #conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    extracted_data[source_table].to_sql(
                        temp_table_name, 
                        conn, 
                        if_exists='replace', 
                        index=False
                    )
                    logger.info(f"Created temp table {temp_table_name} with {len(extracted_data[source_table])} rows")
                except Exception as e:
                    logger.error(f"Error creating temp table {temp_table_name}: {str(e)}")
                    raise
            
            # Now replace the table names in the SQL query to match our temp table names
            modified_sql = transform_config['sql']
            for source_table in source_tables:
                # Replace references to the table name in SQL with the temp table name
                table_pattern = f"temp_{source_table}"
                # Make sure we're only replacing full table names, not parts of other words
                modified_sql = modified_sql.replace(f"FROM {table_pattern}", f"FROM temp_{source_table}")
                modified_sql = modified_sql.replace(f"JOIN {table_pattern}", f"JOIN temp_{source_table}")
            
            logger.info(f"Executing SQL: {modified_sql}")
            
            # Execute transformation SQL
            try:
                result = pd.read_sql(modified_sql, conn)
                logger.info(f"SQL execution returned {len(result)} rows")
            except Exception as e:
                logger.error(f"Error executing SQL: {str(e)}")
                # Check if temp tables exist
                for source_table in source_tables:
                    try:
                        count = pd.read_sql(f"SELECT COUNT(*) FROM temp_{source_table}", conn)
                        logger.info(f"temp_{source_table} has {count.iloc[0, 0]} rows")
                    except Exception as check_e:
                        logger.error(f"Error checking temp_{source_table}: {str(check_e)}")
                raise
    else:
        # Otherwise just use mapping configuration to transform
        source_df = extracted_data[source_tables[0]]
        
        # Apply column mappings
        mappings = transform_config.get('mappings', {})
        result = pd.DataFrame()
        
        for target_col, source_info in mappings.items():
            if isinstance(source_info, str):
                # Simple column mapping
                result[target_col] = source_df[source_info]
            elif isinstance(source_info, dict):
                # Complex transformation
                if 'expression' in source_info:
                    # Apply Python expression
                    result[target_col] = eval(source_info['expression'], 
                                             {'df': source_df, 'np': np, 'pd': pd})
    
    # Add SCD Type 2 fields if not present
    if not 'fecha_inicio_validez' in result.columns:
        result['fecha_inicio_validez'] = datetime.now()
    if not 'fecha_fin_validez' in result.columns:
        result['fecha_fin_validez'] = pd.Timestamp.max
    if not 'flag_registro_actual' in result.columns:
        result['flag_registro_actual'] = True
    if not 'fecha_ultima_modificacion' in result.columns:
        result['fecha_ultima_modificacion'] = datetime.now()
        
    return result

def transform_generic_fact(extracted_data, transformed_data, table_name, transform_config, config):
    """
    Apply generic fact transformation based on configuration
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        transformed_data (dict): Dictionary of already transformed dataframes
        table_name (str): Name of the fact table
        transform_config (dict): Transformation configuration
        config (dict): ETL configuration
        
    Returns:
        DataFrame: Transformed fact data
    """
    # Get source tables needed for this fact
    source_tables = transform_config.get('source_tables', [])
    
    # Check if we have all required source data
    for source_table in source_tables:
        if source_table not in extracted_data:
            logger.error(f"Missing required source data: {source_table}")
            raise ValueError(f"Missing required source data: {source_table}")
    
    # Create temporary tables for each source and transformed dataframe
    with config['source']['engine'].connect() as conn:
        # Log available tables
        logger.info(f"Available source tables for fact transformation: {list(extracted_data.keys())}")
        logger.info(f"Available dimension tables: {list(transformed_data['dimensions'].keys())}")
        
        # Create temp tables for extracted data
        for source_table in source_tables:
            temp_table_name = f"temp_{source_table}"
            logger.info(f"Creating temporary table for fact: {temp_table_name}")
            
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))
                extracted_data[source_table].to_sql(
                    temp_table_name, 
                    conn, 
                    if_exists='replace', 
                    index=False
                )
                logger.info(f"Created temp table {temp_table_name} with {len(extracted_data[source_table])} rows")
            except Exception as e:
                logger.error(f"Error creating temp table {temp_table_name}: {str(e)}")
                raise
        
        # Create temp tables for transformed dimensions
        for dim_name, dim_df in transformed_data['dimensions'].items():
            temp_dim_name = f"temp_{dim_name}"
            logger.info(f"Creating temporary table for dimension: {temp_dim_name}")
            
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_dim_name}"))
                dim_df.to_sql(
                    temp_dim_name, 
                    conn, 
                    if_exists='replace', 
                    index=False
                )
                logger.info(f"Created temp dimension table {temp_dim_name} with {len(dim_df)} rows")
            except Exception as e:
                logger.error(f"Error creating temp dimension table {temp_dim_name}: {str(e)}")
                raise
        
        # Execute transformation SQL
        if 'sql' in transform_config:
            # Replace table references to match our temp tables
            modified_sql = transform_config['sql']
            
            # Modify SQL to use our temp table names
            for source_table in source_tables:
                # Replace references to the table name in SQL with the temp table name
                table_pattern = f"temp_{source_table}"
                modified_sql = modified_sql.replace(f"FROM {table_pattern}", f"FROM temp_{source_table}")
                modified_sql = modified_sql.replace(f"JOIN {table_pattern}", f"JOIN temp_{source_table}")
            
            logger.info(f"Executing fact SQL: {modified_sql}")
            
            try:
                result = pd.read_sql(modified_sql, conn)
                logger.info(f"SQL execution returned {len(result)} rows for fact table")
            except Exception as e:
                logger.error(f"Error executing fact SQL: {str(e)}")
                raise
        else:
            # Default transformation if no SQL provided
            logger.warning(f"No SQL transformation defined for {table_name}, using default transformation")
            result = transform_default_fact(extracted_data, transformed_data, table_name, transform_config)
    
    return result

def transform_default_fact(extracted_data, transformed_data, table_name, transform_config):
    """
    Apply default transformation for fact tables when no specific SQL is provided
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        transformed_data (dict): Dictionary of transformed dataframes
        table_name (str): Name of the fact table
        transform_config (dict): Transformation configuration
        
    Returns:
        DataFrame: Transformed fact data
    """
    # This is a simple placeholder implementation
    # In a real scenario, you would implement specific logic for each fact table
    source_tables = transform_config.get('source_tables', [])
    if not source_tables:
        return pd.DataFrame()
    
    # Start with the first source table
    result = extracted_data[source_tables[0]].copy()
    
    # Apply mappings if provided
    mappings = transform_config.get('mappings', {})
    if mappings:
        new_df = pd.DataFrame()
        for target_col, source_info in mappings.items():
            if isinstance(source_info, str):
                # Simple column mapping
                new_df[target_col] = result[source_info]
            elif isinstance(source_info, dict):
                # Complex transformation
                if 'expression' in source_info:
                    # Apply Python expression
                    new_df[target_col] = eval(source_info['expression'], 
                                             {'df': result, 'np': np, 'pd': pd})
        result = new_df
    
    return result

# Specific transformation functions for dimension tables
def transform_dimtiempo(extracted_data, config, last_run_timestamp=None):
    """
    Transform time dimension table incrementally
    """
    logger.info("Generating DimTiempo incrementally")
    
    # Determinar rango de fechas a procesar
    if last_run_timestamp:
        start_date = last_run_timestamp
    else:
        start_date = pd.to_datetime(config.get('dimtiempo_start_date', '2020-01-01'))
    
    end_date = pd.Timestamp.now().floor('H')  # Hasta la hora actual
    
    # Solo procesar si hay nuevas horas
    if start_date >= end_date:
        logger.info("No new time periods to process")
        return pd.DataFrame()
    
    # Generate date range (solo nuevos períodos)
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')
    
    # Create dataframe con nombres en minúsculas
    df = pd.DataFrame({
        #'dk_tiempo': date_range.strftime('%Y%m%d%H').astype(int),
        'id_fecha_completa': date_range.strftime('%Y%m%d%H').astype('int64'),
        'fecha_completa': date_range,
        'anio': date_range.year,
        'semestre': ((date_range.quarter - 1) // 2 + 1),
        'trimestre': date_range.quarter,
        'mes': date_range.month,
        'semana': date_range.isocalendar().week,
        'dia': date_range.day,
        'dia_semana': date_range.dayofweek,
        'es_fin_semana': date_range.dayofweek >= 5,
        'es_feriado': False,
        'hora': date_range.hour,
        'minuto': 0,  # Como es por hora, minuto siempre 0
        'periodo_dia': pd.cut(
            date_range.hour,
            bins=[0, 12, 18, 24],
            labels=['Mañana', 'Tarde', 'Noche'],
            include_lowest=True,
            right=False
        ),
        'anio_mes': date_range.strftime('%Y%m').astype(int)
    })
    
    # Add SCD Type 2 fields
    current_datetime = datetime.now()
    df['fecha_creacion'] = current_datetime
    df['fecha_inicio_validez'] = current_datetime
    df['fecha_fin_validez'] = pd.Timestamp.max
    df['flag_registro_actual'] = True
    df['fecha_ultima_modificacion'] = current_datetime
    
    return df

def transform_fact_servicios(extracted_data, transformed_data, config):
    """
    Transform fact_servicios table using pandas for better performance
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        transformed_data (dict): Dictionary of transformed dataframes
        config (dict): ETL configuration
        
    Returns:
        DataFrame: Transformed fact table
    """
    logger.info("Transforming fact_servicios with optimized pandas logic")
    
    # Check required source data
    required_tables = ['servicio', 'estados_servicio', 'estado', 'tipo_servicio']
    for table in required_tables:
        if table not in extracted_data:
            logger.error(f"Missing required source data: {table}")
            raise ValueError(f"Missing required source data: {table}")
    
    # Get source dataframes
    df_servicio = extracted_data['servicio'].copy()
    df_estados_servicio = extracted_data['estados_servicio'].copy()
    df_estado = extracted_data['estado'].copy()
    df_tipo_servicio = extracted_data['tipo_servicio'].copy()
    
    logger.info(f"Processing {len(df_servicio)} services")
    
    # Merge estados_servicio with estado to get state names
    df_estados_with_names = df_estados_servicio.merge(
        df_estado[['estado_id', 'nombre']], 
        on='estado_id', 
        how='left'
    )
    
    # Use safe datetime conversion
    df_estados_with_names['fecha_hora'] = safe_datetime_conversion(
        df_estados_with_names['fecha'], 
        df_estados_with_names['hora'], 
        "estados_servicio_fecha_hora"
    )
    
    df_servicio['fecha_hora_solicitud'] = safe_datetime_conversion(
        df_servicio['fecha_solicitud'], 
        df_servicio['hora_solicitud'], 
        "servicio_fecha_hora_solicitud"
    )
    
    df_servicio['fecha_hora_deseada'] = safe_datetime_conversion(
        df_servicio['fecha_deseada'], 
        df_servicio['hora_deseada'], 
        "servicio_fecha_hora_deseada"
    )
    
    # Calculate aggregated information per service
    logger.info("Calculating service states aggregations...")
    
    # Get last update time per service
    ultima_actualizacion = df_estados_with_names.groupby('servicio_id').agg({
        'fecha_hora': 'max'
    }).reset_index()
    ultima_actualizacion.columns = ['servicio_id', 'ultima_actualizacion']
    
    # Get current state (most recent) per service
    estado_actual = df_estados_with_names.loc[
        df_estados_with_names.groupby('servicio_id')['fecha_hora'].idxmax()
    ][['servicio_id', 'nombre']].rename(columns={'nombre': 'estado_actual'})
    
    # Define state categories for calculations
    estados_asignacion = ['Con mensajero Asignado', 'Iniciado']
    estados_completado = ['Entregado en destino', 'Terminado completo']
    estados_cancelado = ['Con novedad']
    
    # Get first assignment time per service
    df_asignacion = df_estados_with_names[
        df_estados_with_names['nombre'].isin(estados_asignacion)
    ]
    primera_asignacion = df_asignacion.groupby('servicio_id').agg({
        'fecha_hora': 'min'
    }).reset_index()
    primera_asignacion.columns = ['servicio_id', 'primera_asignacion']
    
    # Get first completion time per service
    df_completado = df_estados_with_names[
        df_estados_with_names['nombre'].isin(estados_completado)
    ]
    primera_completado = df_completado.groupby('servicio_id').agg({
        'fecha_hora': 'min'
    }).reset_index()
    primera_completado.columns = ['servicio_id', 'primera_completado']
    
    # Check for flags
    servicios_completados = set(df_completado['servicio_id'].unique())
    servicios_cancelados = set(df_estados_with_names[
        df_estados_with_names['nombre'].isin(estados_cancelado)
    ]['servicio_id'].unique())
    
    # Start building the result dataframe
    result = df_servicio.copy()
    
    # Add aggregated information
    result = result.merge(ultima_actualizacion, on='servicio_id', how='left')
    result = result.merge(estado_actual, on='servicio_id', how='left')
    result = result.merge(primera_asignacion, on='servicio_id', how='left')
    result = result.merge(primera_completado, on='servicio_id', how='left')
    
    # Add tipo_servicio information
    result = result.merge(
        df_tipo_servicio[['tipo_servicio_id', 'nombre']].rename(columns={'nombre': 'tipo_servicio'}),
        on='tipo_servicio_id', 
        how='left'
    )
    
    # Build the final structure according to the original SQL
    logger.info("Building final fact table structure...")
    
    fact_servicios = pd.DataFrame()
    
    # Basic IDs
    fact_servicios['sk_servicio'] = pd.Series([None] * len(result))  # NULL as in SQL
    fact_servicios['id_servicio_bdo'] = result['servicio_id']
    fact_servicios['id_cliente_bdo'] = result['cliente_id']
    fact_servicios['id_usuario_bdo'] = result['usuario_id']
    fact_servicios['id_mensajero_principal_bdo'] = result['mensajero_id']
    fact_servicios['id_mensajero_secundario_bdo'] = result['mensajero2_id']
    fact_servicios['id_mensajero_terciario_bdo'] = result['mensajero3_id']
    fact_servicios['id_direccion_origen_bdo'] = result['origen_id']
    fact_servicios['id_direccion_destino_bdo'] = result['destino_id']
    fact_servicios['id_tipopago_bdo'] = result['tipo_pago_id']
    fact_servicios['id_tipovehiculo_bdo'] = result['tipo_vehiculo_id']
    
    # Time dimension keys (format YYYYMMDDHH) - MODIFICACIÓN: Solo horas, sin minutos
    fact_servicios['id_tiempo_solicitud'] = format_datetime_for_key(result['fecha_hora_solicitud'])
    fact_servicios['id_tiempo_deseado'] = format_datetime_for_key(result['fecha_hora_deseada'])
    fact_servicios['id_tiempo_ultima_actualizacion'] = format_datetime_for_key(result['ultima_actualizacion'])
    
    # Service information
    fact_servicios['estado_actual'] = result['estado_actual']
    fact_servicios['tipo_servicio'] = result['tipo_servicio']
    fact_servicios['descripcion_servicio'] = result['descripcion']
    fact_servicios['nombre_solicitante'] = result['nombre_solicitante']
    fact_servicios['nombre_recibe'] = result['nombre_recibe']
    fact_servicios['telefono_recibe'] = result['telefono_recibe']
    fact_servicios['descripcion_pago'] = result['descripcion_pago']
    fact_servicios['flag_ida_y_regreso'] = result['ida_y_regreso']
    fact_servicios['prioridad'] = result['prioridad']
    fact_servicios['flag_multiples_origenes'] = result['multiples_origenes']
    
    # Time calculations in minutes
    logger.info("Calculating time metrics...")
    
    # Total service time (from request to last update)
    fact_servicios['tiempo_total_servicio_minutos'] = (
        (result['ultima_actualizacion'] - result['fecha_hora_solicitud']).dt.total_seconds() / 60
    ).where(result['ultima_actualizacion'].notna())
    
    # Assignment time (from request to first assignment)
    fact_servicios['tiempo_asignacion_minutos'] = (
        (result['primera_asignacion'] - result['fecha_hora_solicitud']).dt.total_seconds() / 60
    ).where(result['primera_asignacion'].notna())
    
    # Delivery time (from assignment to completion)
    fact_servicios['tiempo_entrega_minutos'] = (
        (result['primera_completado'] - result['primera_asignacion']).dt.total_seconds() / 60
    ).where(
        (result['primera_completado'].notna()) & (result['primera_asignacion'].notna())
    )
    
    # Status flags
    fact_servicios['flag_completado'] = result['servicio_id'].isin(servicios_completados)
    fact_servicios['flag_cancelado'] = result['servicio_id'].isin(servicios_cancelados)
    
    # Audit information
    fact_servicios['flag_activo'] = result['activo']
    fact_servicios['flag_es_prueba'] = result['es_prueba']
    fact_servicios['fecha_creacion'] = pd.Timestamp.now()
    fact_servicios['fecha_ultima_modificacion'] = pd.Timestamp.now()
    
    # Sort by service ID as in original SQL
    fact_servicios = fact_servicios.sort_values('id_servicio_bdo').reset_index(drop=True)
    
    logger.info(f"Generated {len(fact_servicios)} rows for fact_servicios")
    
    return fact_servicios

def safe_datetime_conversion(fecha_series, hora_series, column_name="datetime"):
    """
    Safely convert fecha and hora series to datetime with multiple fallback strategies
    
    Args:
        fecha_series: pandas Series with date values
        hora_series: pandas Series with time values
        column_name: name for logging purposes
        
    Returns:
        pandas Series with datetime values
    """
    logger.info(f"Converting {column_name} with {len(fecha_series)} records")
    
    # Combine fecha and hora into strings
    datetime_strings = fecha_series.astype(str) + ' ' + hora_series.astype(str)
    
    # Strategy 1: Try with mixed format (most flexible)
    try:
        result = pd.to_datetime(datetime_strings, format='mixed', errors='raise')
        logger.info(f"Successfully converted {column_name} using mixed format")
        return result
    except Exception as e1:
        logger.warning(f"Mixed format failed for {column_name}: {str(e1)}")
    
    # Strategy 2: Try with coerce (convert errors to NaT)
    try:
        result = pd.to_datetime(datetime_strings, errors='coerce')
        failed_count = result.isna().sum()
        success_count = len(result) - failed_count
        logger.info(f"Coerce conversion for {column_name}: {success_count} successful, {failed_count} failed")
        
        # Log examples of failed conversions
        if failed_count > 0:
            failed_mask = result.isna()
            failed_examples = datetime_strings[failed_mask].head(5).tolist()
            logger.warning(f"Examples of failed {column_name} conversions: {failed_examples}")
        
        return result
    except Exception as e2:
        logger.warning(f"Coerce conversion failed for {column_name}: {str(e2)}")
    
    # Strategy 3: Try to clean the data first
    try:
        logger.info(f"Attempting to clean {column_name} data before conversion")
        
        # Clean common issues
        cleaned_strings = datetime_strings.str.replace(r'\.\d+$', '', regex=True)  # Remove microseconds
        cleaned_strings = cleaned_strings.str.strip()  # Remove whitespace
        
        result = pd.to_datetime(cleaned_strings, errors='coerce')
        failed_count = result.isna().sum()
        success_count = len(result) - failed_count
        logger.info(f"Cleaned conversion for {column_name}: {success_count} successful, {failed_count} failed")
        
        return result
    except Exception as e3:
        logger.error(f"All conversion strategies failed for {column_name}: {str(e3)}")
    
    # Strategy 4: Last resort - return NaT series
    logger.error(f"Returning NaT series for {column_name}")
    return pd.Series([pd.NaT] * len(fecha_series), index=fecha_series.index)

def format_datetime_for_key(datetime_series, format_str='%Y%m%d%H'):
    """
    Safely format datetime series to string keys, handling NaT values
    MODIFICACIÓN: Cambiado el formato de '%Y%m%d%H%M' a '%Y%m%d%H' para solo mostrar horas
    
    Args:
        datetime_series: pandas Series with datetime values
        format_str: strftime format string (default: '%Y%m%d%H' - solo horas)
        
    Returns:
        pandas Series with formatted strings (or None for NaT values)
    """
    try:
        # Handle NaT values by converting to None
        result = datetime_series.dt.strftime(format_str)
        # Convert empty strings back to None
        result = result.where(result != '', None)
        # Try to convert to Int64 (nullable integer)
        return result.astype('Int64')
    except Exception as e:
        logger.warning(f"Error formatting datetime series: {str(e)}")
        # Return None series as fallback
        return pd.Series([None] * len(datetime_series), index=datetime_series.index, dtype='Int64')