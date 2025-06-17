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
    df['fecha_inicio_validez'] = current_datetime
    df['fecha_fin_validez'] = pd.Timestamp.max
    df['flag_registro_actual'] = True
    df['fecha_ultima_modificacion'] = current_datetime
    
    return df

# Add more specific transformation functions as needed for other dimensions and facts
def transform_factserviciosmensajeria(extracted_data, transformed_data, config):
    """
    Transform FactServiciosMensajeria
    
    Args:
        extracted_data (dict): Dictionary of extracted dataframes
        transformed_data (dict): Dictionary of transformed dataframes
        config (dict): ETL configuration
        
    Returns:
        DataFrame: Transformed fact table
    """
    # Implementation would depend on the specific business logic
    # This is a placeholder that should be customized based on actual requirements
    
    logger.info("Transforming FactServiciosMensajeria with specific logic")
    
    # For example, a simple transformation could look like this:
    if 'servicio' not in extracted_data:
        logger.error("Missing required source data: servicio")
        return pd.DataFrame()
    
    # Start with source data
    df_servicios = extracted_data['servicio'].copy()
    
    # Create a result dataframe with the required structure
    result = pd.DataFrame()
    
    # Generate surrogate key
    result['SK_Servicio'] = range(1, len(df_servicios) + 1)
    
    # Map foreign keys (would need to be adjusted based on real data)
    # In a real implementation, you would look up these keys from the transformed dimensions
    result['DK_Cliente'] = df_servicios['cliente_id']  # Replace with actual lookup
    result['DK_TipoServicio'] = df_servicios['tipo_servicio_id']  # Replace with actual lookup
    result['DK_TipoPago'] = df_servicios['tipo_pago_id']  # Replace with actual lookup
    result['DK_TipoVehiculo'] = df_servicios['tipo_vehiculo_id']  # Replace with actual lookup
    result['DK_OrigenServicio'] = df_servicios['origen_id']  # Replace with actual lookup
    result['DK_DestinoServicio'] = df_servicios['destino_id']  # Replace with actual lookup
    
    # Convert service date to timestamp and look up in DimTiempo
    # This would need a proper lookup in the real implementation
    result['DK_Tiempo'] = 1  # Placeholder
    
    # Map metrics
    result['Costo_Total'] = df_servicios['costo_total']
    result['Distancia_Recorrida'] = df_servicios['distancia_km']
    result['Tiempo_Total_Estimado'] = df_servicios['tiempo_estimado_minutos']
    result['Flag_Cumplimiento'] = df_servicios['cumplido_flag']
    result['Valor_Descuento'] = df_servicios['descuento']
    result['ID_Servicio_BDO'] = df_servicios['servicio_id']
    
    return result
