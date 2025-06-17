"""
Data loading module for ETL process
Loads transformed data into the data warehouse
"""
import logging
import numpy as np
from sqlalchemy import inspect
from datetime import datetime
import pandas as pd
from sqlalchemy import Table, Column, Integer, String, DateTime, Float, Boolean, MetaData, ForeignKey, text, BigInteger
from sqlalchemy.dialects.postgresql import TIMESTAMP

logger = logging.getLogger(__name__)

def load_data(transformed_data, config):
    """
    Load transformed data into the data warehouse
    
    Args:
        transformed_data (dict): Dictionary of transformed dataframes
        config (dict): ETL configuration
        
    Returns:
        dict: Load results statistics
    """
    logger.info("Starting data loading")
    results = {
        'tables_loaded': 0,
        'rows_loaded': 0,
        'rows_updated': 0,
        'rows_skipped': 0
    }
    
    try:
        warehouse_engine = config['warehouse']['engine']
        warehouse_metadata = config['warehouse']['metadata']
        
        # Load dimension tables first
        for dim_name, dim_df in transformed_data['dimensions'].items():
            if not dim_df.empty:
                load_result = load_dimension_table(dim_name, dim_df, warehouse_engine, warehouse_metadata, config)
                results['tables_loaded'] += 1
                results['rows_loaded'] += load_result['rows_loaded']
                results['rows_updated'] += load_result['rows_updated']
                results['rows_skipped'] += load_result['rows_skipped']
                logger.info(f"Dimension {dim_name}: {load_result['rows_loaded']} loaded, {load_result['rows_updated']} updated, {load_result['rows_skipped']} skipped")
        
        
        # Then load fact tables
        for fact_name, fact_df in transformed_data['facts'].items():
            if not fact_df.empty:
                load_result = load_fact_table(fact_name, fact_df, warehouse_engine, warehouse_metadata, config)
                results['tables_loaded'] += 1
                results['rows_loaded'] += load_result['rows_loaded']
                results['rows_updated'] += load_result['rows_updated']
                results['rows_skipped'] += load_result['rows_skipped']
                logger.info(f"Fact {fact_name}: {load_result['rows_loaded']} loaded, {load_result['rows_updated']} updated, {load_result['rows_skipped']} skipped")
        
        # Update ETL tracking information
        update_etl_tracking(warehouse_engine, datetime.now(), results, config)
        
        return results
    
    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}", exc_info=True)
        raise

def load_dimension_table(table_name, df, engine, metadata, config):
    """
    Load data into a dimension table using SCD Type 2
    
    Args:
        table_name (str): Name of the dimension table
        df (DataFrame): Data to load
        engine: SQLAlchemy engine
        metadata: SQLAlchemy metadata
        config (dict): ETL configuration
        
    Returns:
        dict: Load results with counts
    """
    result = {
        'rows_loaded': 0,
        'rows_updated': 0,
        'rows_skipped': 0
    }

    df = normalize_dataframe_columns(df)

    df = df.replace({np.nan: None})
    
    try:
        # Define the table if it doesn't exist
        table_exists = inspect(engine).has_table(table_name)
        if not table_exists:
            create_dimension_table(table_name, engine, metadata, config)
            with engine.connect() as conn:
                conn.commit()
            metadata.clear()
            import time
            time.sleep(1)

        #Get business keys for this dimension
        business_keys = get_dimension_business_keys(table_name, config)
        pk_column = get_table_primary_key(table_name)


        with engine.connect() as conn:
            # Start transaction
            with conn.begin():
                for _, row in df.iterrows():
                    # Check if record exists using business keys
                    existing_record = check_dimension_exists(conn, table_name, row, business_keys)
                    if existing_record:
                        logger.debug(f"Ingresé al cambio")
                        if has_dimension_changed(conn, table_name, row, existing_record, business_keys):
                            expire_current_record(conn, table_name, existing_record[pk_column])
                            insert_new_dimension_record (conn, table_name, row, exclude_pk = True)
                            result['rows_update'] += 1
                            logger.debug(f"Updated record in {table_name} with business keys: {[row[key] for key in business_keys if key in row]}")
                        else:
                            result['rows_skipped'] += 1
                            logger.debug(f"Skipped unchanged record in {table_name}")
                    else:
                        insert_new_dimension_record (conn, table_name, row, exclude_pk = True)
                        result['rows_loaded'] += 1
                        logger.debug(f"Inserted new record in {table_name}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error loading dimension table {table_name}: {str(e)}", exc_info=True)
        raise

def load_fact_table(table_name, df, engine, metadata, config):
    """
    Load data into a fact table
    
    Args:
        table_name (str): Name of the fact table
        df (DataFrame): Data to load
        engine: SQLAlchemy engine
        metadata: SQLAlchemy metadata
        config (dict): ETL configuration
        
    Returns:
        dict: Load results with counts
    """
    result = {
        'rows_loaded': 0,
        'rows_updated': 0,
        'rows_skipped': 0
    }

    if table_name == "fact_servicios":
        df = map_keys_for_fact_servicios(df, engine)
    elif table_name == "fact_novedades":
        df = map_keys_for_fact_novedades(df, engine)
    elif table_name == "fact_estados_servicio":
        df = map_keys_for_fact_estados_servicios(df, engine)
    
    # Se convierten los nombres a PascalCase
    df = normalize_dataframe_columns(df)
    df = df.replace({np.nan: None})
    
    try:
        # Define the table if it doesn't exist
        table_exists = inspect(engine).has_table(table_name)
        if not table_exists:
            create_fact_table(table_name, engine, metadata, config)
            with engine.connect() as conn:
                conn.commit()
            metadata.clear()
            import time
            time.sleep(1)
        
        # Get unique columns for this fact table
        unique_columns = get_fact_table_unique_columns(table_name, config)
        pk_column = get_table_primary_key(table_name)
        
        with engine.connect() as conn:
            # Start transaction
            with conn.begin():
                for _, row in df.iterrows():
                    # Check if record exists using unique columns
                    existing_record = check_fact_exists(conn, table_name, row, unique_columns)
                    
                    if existing_record:
                        # For facts, you might want to update or skip
                        # Here we'll skip duplicates (most common approach)
                        result['rows_skipped'] += 1
                        logger.debug(f"Skipped duplicate record in {table_name}")
                    else:
                        # Insert new record
                        insert_new_fact_record(conn, table_name, row, exclude_pk=True)
                        result['rows_loaded'] += 1
                        logger.debug(f"Inserted new record in {table_name}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error loading fact table {table_name}: {str(e)}", exc_info=True)
        raise

def check_dimension_exists(conn, table_name, row, business_keys):
    """
    Check if a dimension record exists based on business keys
    
    Args:
        conn: Database connection
        table_name (str): Name of the table
        row (Series): DataFrame row
        business_keys (list): List of business key columns
        
    Returns:
        dict or None: Existing record if found, None otherwise
    """
    # Build WHERE clause using business keys that exist in the row
    where_conditions = []
    params = {}
    
    for key in business_keys:
        # Check both original and normalized column names
        key_pascal = normalize_column_name(key)
        if key in row and row[key] is not None:
            where_conditions.append(f"{key_pascal} = :{key}")
            params[key] = row[key]
        elif key_pascal in row and row[key_pascal] is not None:
            where_conditions.append(f"{key_pascal} = :{key}")
            params[key] = row[key_pascal]
    
    if not where_conditions:
        return None
    
    where_clause = " AND ".join(where_conditions)
    query = f"""
    SELECT * FROM {table_name}
    WHERE {where_clause}
    AND Flag_Registro_Actual = TRUE
    """
    
    try:
        result = conn.execute(text(query), params).fetchone()
        return dict(result._mapping) if result else None
    except Exception as e:
        logger.warning(f"Error checking dimension existence: {str(e)}")
        return None

def check_fact_exists(conn, table_name, row, unique_columns):
    """
    Check if a fact record exists based on unique columns
    
    Args:
        conn: Database connection
        table_name (str): Name of the table
        row (Series): DataFrame row
        unique_columns (list): List of unique columns
        
    Returns:
        dict or None: Existing record if found, None otherwise
    """
    # Build WHERE clause using unique columns that exist in the row
    where_conditions = []
    params = {}
    
    for key in unique_columns:
        # Check both original and normalized column names
        key_pascal = normalize_column_name(key)
        if key in row and row[key] is not None:
            where_conditions.append(f"{key_pascal} = :{key}")
            params[key] = row[key]
        elif key_pascal in row and row[key_pascal] is not None:
            where_conditions.append(f"{key_pascal} = :{key}")
            params[key] = row[key_pascal]
    
    if not where_conditions:
        return None
    
    where_clause = " AND ".join(where_conditions)
    query = f"""
    SELECT * FROM {table_name}
    WHERE {where_clause}
    """
    
    try:
        result = conn.execute(text(query), params).fetchone()
        return dict(result._mapping) if result else None
    except Exception as e:
        logger.warning(f"Error checking fact existence: {str(e)}")
        return None

def has_dimension_changed(conn, table_name, new_row, existing_record, business_keys):
    """
    Check if dimension data has changed (for SCD Type 2)
    
    Args:
        conn: Database connection
        table_name (str): Name of the table
        new_row (Series): New data row
        existing_record (dict): Existing record from database
        business_keys (list): Business key columns to exclude from comparison
        
    Returns:
        bool: True if data has changed
    """
    # Compare all columns except business keys, audit fields, and primary key
    exclude_columns = set(business_keys + [
        get_table_primary_key(table_name),
        'Flag_Registro_Actual', 'Fecha_Inicio_Validez', 'Fecha_Fin_Validez',
        'Fecha_Creacion', 'Fecha_Ultima_Modificacion'
    ])
    
    for col in new_row.index:
        col_pascal = normalize_column_name(col)
        if col_pascal in exclude_columns:
            continue
            
        if col_pascal in existing_record:
            new_value = new_row[col] if col in new_row else new_row.get(col_pascal)
            existing_value = existing_record[col_pascal]
            
            # Handle None/null comparisons
            if new_value != existing_value:
                # Handle special cases for numeric comparisons
                if isinstance(new_value, (int, float)) and isinstance(existing_value, (int, float)):
                    if abs(new_value - existing_value) > 1e-10:  # Small tolerance for float comparison
                        return True
                else:
                    return True
    
    return False

def expire_current_record(conn, table_name, pk_value):
    """
    Expire current record by setting Flag_Registro_Actual to False
    """
    update_query = f"""
    UPDATE {table_name}
    SET Flag_Registro_Actual = FALSE,
        Fecha_Fin_Validez = :current_datetime,
        Fecha_Ultima_Modificacion = :current_datetime
    WHERE {get_table_primary_key(table_name)} = :pk_value
    AND Flag_Registro_Actual = TRUE
    """
    
    conn.execute(text(update_query), {
        'current_datetime': datetime.now(),
        'pk_value': pk_value
    })

def insert_new_dimension_record(conn, table_name, row, exclude_pk=False):
    """
    Insert new dimension record with SCD Type 2 fields
    """

    row_dict = row.to_dict()

    if exclude_pk:
        pk_column = get_table_primary_key(table_name)
        pk_column_pascal = normalize_column_name(pk_column)
        if pk_column in row_dict:
            del row_dict[pk_column]
        if pk_column_pascal in row_dict:
            del row_dict[pk_column_pascal]

    columns = list(row_dict.keys())
    columns_str = ", ".join(columns)
    placeholders_str = ", ".join([f":{col}" for col in columns])
    
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"
    conn.execute(text(insert_query), row_dict)

def insert_new_fact_record(conn, table_name, row, exclude_pk=False):
    """
    Insert new fact record
    """
    row_dict = row.to_dict()
    
    if exclude_pk:
        pk_column = get_table_primary_key(table_name)
        pk_column_pascal = normalize_column_name(pk_column)
        if pk_column in row_dict:
            del row_dict[pk_column]
        if pk_column_pascal in row_dict:
            del row_dict[pk_column_pascal]
    
    # Add audit fields for facts
    current_time = datetime.now()
    row_dict.update({
        'Fecha_Creacion': current_time,
        'Fecha_Ultima_Modificacion': current_time
    })
    
    # Build insert query
    columns = list(row_dict.keys())
    columns_str = ", ".join(columns)
    placeholders_str = ", ".join([f":{col}" for col in columns])
    
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"
    conn.execute(text(insert_query), row_dict)

def normalize_column_name(column_name):
    """
    Convert column name to PascalCase
    """
    parts = column_name.split('_')
    return '_'.join(p.capitalize() for p in parts)

def get_insert_query(table_name, row, exclude_pk=False):
    """
    Generate SQL INSERT query for a row
    
    Args:
        table_name (str): Name of the table
        row (Series): DataFrame row
        exclude_pk (bool): Whether to exclude primary key
    
    Returns:
        str: SQL INSERT query
    """
    columns = []
    
    if exclude_pk:
        pk_column = get_table_primary_key(table_name)
        columns = [col for col in row.index if col != pk_column and col.lower() != pk_column.lower()]
    else:
        columns = row.index
    
    columns_str = ", ".join(columns)
    placeholders_str = ", ".join([f":{col}" for col in columns])
    
    return f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str})"

def get_dimension_business_keys(table_name, config):
    """
    Get business key columns that identify unique dimension records
    """
    dim_config = config.get('dimension_tables', {}).get(table_name, {})
    business_keys = dim_config.get('business_keys', [])
    
    if not business_keys:
        # Common business key patterns
        common_keys = ['codigo', 'nombre', 'code', 'name', 'id_externo', 'external_id']
        business_keys = common_keys
    
    return business_keys

def get_fact_table_unique_columns(table_name, config):
    """
    Get columns that make a fact record unique
    """
    fact_config = config.get('fact_tables', {}).get(table_name, {})
    unique_columns = fact_config.get('unique_columns', [])
    
    if not unique_columns:
        # Common unique column patterns for facts
        common_unique = [
            'fecha', 'id_cliente', 'id_producto', 'id_sucursal',
            'date', 'customer_id', 'product_id', 'store_id',
            'timestamp', 'transaction_id'
        ]
        unique_columns = common_unique
    
    return unique_columns

def normalize_dataframe_columns(df):
    """
    Normaliza las columnas del dataframe para evitar duplicados
    
    Args:
        df: DataFrame a normalizar
        
    Returns:
        DataFrame normalizado
    """
    # Crear un mapa de columnas normalizadas
    normalized_columns = {}
    columns_to_drop = []
    
    for col in df.columns:
        # Convertir a PascalCase (primera letra de cada palabra en mayúscula)
        parts = col.split('_')
        pascal_case = '_'.join(p.capitalize() for p in parts)
        
        # Si ya existe una versión normalizada de esta columna, marcar para eliminar
        if pascal_case in normalized_columns.values():
            columns_to_drop.append(col)
        else:
            normalized_columns[col] = pascal_case
    
    # Eliminar columnas duplicadas
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
    
    # Renombrar columnas al formato PascalCase
    df = df.rename(columns=normalized_columns)
    
    return df

def create_dimension_table(table_name, engine, metadata, config):
    """
    Create a dimension table if it doesn't exist
    
    Args:
        table_name (str): Name of the dimension table
        engine: SQLAlchemy engine
        metadata: SQLAlchemy metadata
        config (dict): ETL configuration
    """
    logger.info(f"Creating dimension table {table_name}")
    
    # Get table definition from SQL scripts
    table_sql = config['sql_scripts'].get('create_tables', {}).get(table_name)
    
    if table_sql:
        # Execute SQL to create table
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(table_sql))
    else:
        # Define table using SQLAlchemy
        table_definition = get_dimension_table_definition(table_name, metadata)
        if table_definition:
            with engine.begin() as conn:
                table_definition.create(bind=conn, checkfirst=True, schema= 'public')
        else:
            logger.error(f"No table definition found for {table_name}")
            raise ValueError(f"No table definition found for {table_name}")
        
        import time
        time.sleep(0.5)

def create_fact_table(table_name, engine, metadata, config):
    """
    Create a fact table if it doesn't exist
    
    Args:
        table_name (str): Name of the fact table
        engine: SQLAlchemy engine
        metadata: SQLAlchemy metadata
        config (dict): ETL configuration
    """
    logger.info(f"Creating fact table {table_name}")
    
    # Get table definition from SQL scripts
    table_sql = config['sql_scripts'].get('create_tables', {}).get(table_name)
    
    if table_sql:
        # Execute SQL to create table
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(table_sql))
    else:
        # Define table using SQLAlchemy
        table_definition = get_fact_table_definition(table_name, metadata)
        if table_definition:
            with engine.begin() as conn:
                table_definition.create(bind=conn, checkfirst=True, schema='public')
        else:
            logger.error(f"No table definition found for {table_name}")
            raise ValueError(f"No table definition found for {table_name}")
    
    import time
    time.sleep(0.5)

def update_etl_tracking(engine, execution_time, results, config):
    """
    Update ETL tracking information
    
    Args:
        engine: SQLAlchemy engine
        execution_time: Execution timestamp
        results (dict): Load results
        config (dict): ETL configuration
    """
    tracking_table = config['warehouse'].get('tracking_table', 'ETL_Tracking')
    
    # Create tracking table if it doesn't exist
    create_tracking_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {tracking_table} (
        id SERIAL PRIMARY KEY,
        execution_time TIMESTAMP,
        tables_loaded INTEGER,
        rows_loaded INTEGER,
        status VARCHAR(20),
        details TEXT
    )
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_tracking_table_sql))
        
        # Insert tracking record
        insert_sql = f"""
        INSERT INTO {tracking_table} (execution_time, tables_loaded, rows_loaded, status, details)
        VALUES (:execution_time, :tables_loaded, :rows_loaded, :status, :details)
        """
        
        conn.execute(text(insert_sql), {
            'execution_time': execution_time,
            'tables_loaded': results['tables_loaded'],
            'rows_loaded': results['rows_loaded'],
            'status': 'Success',
            'details': f"Loaded {results['tables_loaded']} tables with {results['rows_loaded']} total rows"
        })

def get_dimension_table_definition(table_name, metadata):
    """
    Get SQLAlchemy table definition for a dimension table
    
    Args:
        table_name (str): Name of the dimension table
        metadata: SQLAlchemy metadata
        
    Returns:
        Table: SQLAlchemy Table object
    """
    # Define dimension tables based on the data warehouse design
    if table_name == "dimarea":
        return Table(
            'dimarea', metadata,
            Column('dk_area', Integer, primary_key=True, autoincrement=True),
            Column('id_area_bdo', String(50), nullable=False),
            Column('nombre_area', String(255)),
            Column('descripcion', String(1000)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimtipocliente':
        return Table(
            'dimtipocliente', metadata,
            Column('dk_tipocliente', Integer, primary_key=True, autoincrement=True),
            Column('id_tipocliente_bdo',String(50), nullable=False),
            Column('nombre_tipo_cliente', String(255)),
            Column('descripcion', String(500)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimciudad':
        return Table(
            'dimciudad', metadata,
            Column('dk_ciudad', Integer, primary_key= True, autoincrement= True),
            Column('id_ciudad_bdo', String(50), nullable= False),
            Column('nombre_ciudad', String(255)),
            Column('nombre_departamento', String(255)),
            Column('id_departamento', String(50)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False) 
        )
    
    elif table_name == 'dimcliente':
        return Table(
            'dimcliente', metadata,
            Column('dk_cliente', Integer, primary_key=True, autoincrement=True),
            Column('id_cliente_bdo', String(50), nullable=False),
            Column('nit_cliente', String(50)),
            Column('nombre_cliente', String(100)),
            Column('email', String(100)),
            Column('direccion', String(100)),
            Column('telefono', String(20)),
            Column('nombre_contacto', String(100)),
            Column('id_ciudad_bdo', Integer),
            Column('id_tipocliente_dbo', Integer),
            Column('flag_activo', Boolean, default=True),
            Column('id_coordinador', String(20)),
            Column('sector', String(50)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False) 
        )
    
    elif table_name == 'dimsede':
        return Table(
            'dimsede', metadata,
            Column('dk_sede', Integer, primary_key=True, autoincrement=True),
            Column('id_sede_bdo', String(50), nullable=False),
            Column('nombre_sede', String(100)),
            Column('direccion', String(200)),
            Column('telefono', String(20)),
            Column('nombre_contacto', String(100)),
            Column('id_ciudad_bdo', Integer),
            Column('id_cliente_bdo', Integer),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimmensajero':
        return Table(
            'dimmensajero', metadata,
            Column('dk_mensajero', Integer, primary_key=True, autoincrement=True),
            Column('id_mensajero_bdo', String(50), nullable=False),
            Column('id_usuario', String(50)),
            Column('flag_activo', Boolean, default=True),
            Column('fecha_entrada', DateTime),
            Column('fecha_salida', DateTime),
            Column('salario', Float),
            Column('telefono', String(20)),
            Column('id_ciudad_bdo', Integer),
            Column('fecha_creacion', TIMESTAMP),
            Column('nombre_ciudad_operacion', String(100)),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )

    elif table_name == 'dimtiempo':
        return Table(
            'dimtiempo', metadata,
            Column('dk_tiempo', Integer, primary_key=True, autoincrement=True),
            Column('fecha_completa', DateTime),
            Column('anio', Integer),
            Column('semestre', Integer),
            Column('trimestre', Integer),
            Column('mes', Integer),
            Column('semana', Integer),
            Column('dia', Integer),
            Column('dia_semana', Integer),
            Column('es_fin_semana', Boolean),
            Column('es_feriado', Boolean),
            Column('hora', Integer),
            Column('minuto', Integer),
            Column('periodo_dia', String(20)),
            Column('anio_mes', Integer),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimusuario':
        return Table(
            'dimusuario', metadata,
            Column('dk_usuario', Integer, primary_key= True, autoincrement= True),
            Column('id_usuario_bdo', String(50), nullable= False),
            Column('id_user_sistema', Integer),
            Column('telefono', String(15)),
            Column('dk_area', Integer),
            Column('id_area_bdo', String(150)),
            Column('id_cliente_bdo', Integer),
            Column('nombre_cliente', String(200)),
            Column('id_sede_bdo', Integer),
            Column('nombre_sede', String(200)),
            Column('id_ciudad_bdo', Integer),
            Column('nombre_ciudad', String(200)),
            Column('flag_lider', Boolean),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimdireccion':
        return Table(
            'dimdireccion', metadata,
            Column('dk_direccion', Integer, primary_key= True, autoincrement= True),
            Column('id_direccion_bdo', String(100), nullable= False),
            Column('direccion', String(100)),
            Column('id_ciudad_bdo', Integer),
            Column('nombre_ciudad', String(200)),
            Column('nombre_departamento', String(200)),
            Column('tipo_direccion', String(20)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimtipopago':
        return Table(
            'dimtipopago', metadata,
            Column('dk_tipopago', Integer, primary_key= True, autoincrement= True),
            Column('id_tipopago_bdo', String(50), nullable= False),
            Column('nombre_tipopago', String(100)),
            Column('descripcion', String(200)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimtipovehiculo':
        return Table(
            'dimtipovehiculo', metadata,
            Column('dk_tipovehiculo', Integer, primary_key= True, autoincrement= True),
            Column('id_tipovehiculo_bdo', String(50) ,nullable= False),
            Column('nombre_tipovehiculo', String(200)),
            Column('descripcion', String(200)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    elif table_name == 'dimtiponovedad':
        return Table(
            'dimtiponovedad', metadata,
            Column('dk_tiponovedad', Integer, primary_key= True, autoincrement= True),
            Column('id_tiponovedad_bdo', String(50), nullable= False),
            Column('nombre_tiponovedad', String(150)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_inicio_validez', TIMESTAMP, nullable=False),
            Column('fecha_fin_validez', TIMESTAMP, nullable=False),
            Column('flag_registro_actual', Boolean, nullable=False),
            Column('fecha_ultima_modificacion', TIMESTAMP, nullable=False)
        )
    
    else:
        return None

def get_fact_table_definition(table_name, metadata):
    """
    Get SQLAlchemy table definition for a fact table
    
    Args:
        table_name (str): Name of the fact table
        metadata: SQLAlchemy metadata
        
    Returns:
        Table: SQLAlchemy Table object
    """

    if table_name == 'fact_servicios':
        return Table(
            'fact_servicios', metadata,
            Column('sk_servicio', Integer, primary_key=True, autoincrement=True),
            Column('id_servicio_dbo', Integer, nullable=False),
            Column('dk_cliente', Integer, ForeignKey('dimcliente.dk_cliente')),
            Column('dk_usuario', Integer, ForeignKey('dimusuario.dk_usuario')),
            Column('dk_mensajero_principal', Integer, ForeignKey('dimmensajero.dk_mensajero')),
            Column('dk_mensajero_secundario', Integer, ForeignKey('dimmensajero.dk_mensajero')),
            Column('dk_mensajero_terciario', Integer, ForeignKey('dimmensajero.dk_mensajero')),
            Column('dk_direccion_origen', Integer, ForeignKey('dimdireccion.dk_direccion')),
            Column('dk_direccion_destino', Integer, ForeignKey('dimdireccion.dk_direccion')),
            Column('dk_tipopago', Integer, ForeignKey('dimtipopago.dk_tipopago')),
            Column('dk_tipovehiculo', Integer, ForeignKey('dimtipovehiculo.dk_tipovehiculo')),
            Column('id_tiempo_solicitado', BigInteger),
            Column('id_tiempo_deseado', BigInteger),
            Column('id_tiempo_actualizacion', BigInteger),
            Column('estado_actual', String(200)),
            Column('tipo_servicio', String(200)),
            Column('descripcion_servicio', String(255)),
            Column('nombre_solicitante', String(200)),
            Column('nombre_recibe', String(200)),
            Column('telefono_recibe', String(20)),
            Column('descripcion_pago', String(255)),
            Column('flag_ida_y_regreso', Boolean),
            Column('prioridad', String(100)),
            Column('flag_multiples_origenes', Boolean),
            Column('tiempo_total_servicio_minutos', Float),
            Column('tiempo_asignacion_minutos', Float),
            Column('tiempo_entrega_minutos', Float),
            Column('flag_completado', Boolean),
            Column('flag_cancelado', Boolean),
            Column('flag_activo', Boolean),
            Column('flag_es_prueba', Boolean),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_ultima_actualizacion', TIMESTAMP)

        )
    
    elif table_name == 'fact_estados_servicio':
        return Table(
            'fact_estados_servicio', metadata,
            Column('sk_estado_servicio', Integer, primary_key=True, autoincrement=True),
            Column('id_esatdo_servicio_bdo', Integer, nullable=False),
            Column('sk_servicio', Integer, ForeignKey('fact_servicios.sk_servicio')),
            Column('id_tiempo', BigInteger),
            Column('estado_nombre', String(100)),
            Column('observaciones', String(500)),
            Column('flag_tiene_foto', Boolean),
            Column('flag_es_prueba', Boolean),
            Column('duracion_estado_minutos', Float),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_ultima_modificacion', TIMESTAMP)
        )
    
    elif table_name == 'fact_novedades':
        return Table(
            'fact_novedades', metadata,
            Column('sk_novedad', Integer, primary_key=True, autoincrement=True),
            Column('id_novedad_bdo', Integer, nullable=False),
            Column('sk_servicio', Integer, ForeignKey('fact_servicios.sk_servicio')),
            Column('id_tiempo', BigInteger),
            Column('dk_tiponovedad', Integer, ForeignKey('dimtiponovedad.dk_tiponovedad')),
            Column('dk_mensajero', Integer, ForeignKey('dimmensajero.dk_mensajero')),
            Column('descripcion', String(500)),
            Column('flag_es_prueba', Boolean),
            Column('nombre_tipo_novedad', String(100)),
            Column('fecha_creacion', TIMESTAMP),
            Column('fecha_ultima_modificacion', TIMESTAMP)
        )
    
    else:
        return None

def get_table_primary_key(table_name):
    """
    Get primary key column name for a table
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        str: Primary key column name
    """
    # Map table names to their primary key column names
    pk_map = {
        'dimarea': 'dk_area',
        'dimtipocliente': 'dk_tipocliente',
        'dimciudad': 'dk_ciudad',
        'dimcliente': 'dk_cliente',
        'dimsede': 'dk_sede',
        'dimmensajero': 'dk_mensajero',
        'dimtiempo': 'dk_tiempo',
        'dimusuario': 'dk_usuario',
        'dimdireccion': 'dk_direccion',
        'dimtipopago': 'dk_tipopago',
        'dimtipovehiculo': 'dk_tipovehiculo',
        'dimtiponovedad': 'dk_tiponovedad',
        'fact_servicios': 'sk_servicio',
        'fact_estados_servicio': 'sk_estado_servicio',
        'fact_novedades': 'sk_novedad',
    }
    
    return pk_map.get(table_name)

def map_keys_for_fact_servicios(df, engine):
    """
    Mapea las business keys (id_*_bdo) a surrogate keys (dk_*) 
    y elimina las columnas originales para mantener el DataFrame limpio
    """
    logger.info("Ejecutando mapeo para fact_servicios")
    with engine.connect() as conn:
        # Cargar todas las dimensiones una sola vez
        dim_cliente = pd.read_sql("SELECT id_cliente_bdo, dk_cliente FROM dimcliente", conn)
        dim_usuario = pd.read_sql("SELECT id_usuario_bdo, dk_usuario FROM dimusuario", conn)
        dim_mensajero = pd.read_sql("SELECT id_mensajero_bdo, dk_mensajero FROM dimmensajero", conn)
        dim_direccion = pd.read_sql("SELECT id_direccion_bdo, dk_direccion FROM dimdireccion", conn)
        dim_tipopago = pd.read_sql("SELECT id_tipopago_bdo, dk_tipopago FROM dimtipopago", conn)
        dim_tipovehiculo = pd.read_sql("SELECT id_tipovehiculo_bdo, dk_tipovehiculo FROM dimtipovehiculo", conn)
    
    # Lista para trackear las columnas que vamos a eliminar
    columns_to_drop = []
    
    # Cliente
    if 'id_cliente_bdo' in df.columns:
        df = df.merge(dim_cliente, on='id_cliente_bdo', how='left')
        df['dk_cliente'] = df['dk_cliente'].astype('Int64')
        columns_to_drop.append('id_cliente_bdo')
    
    # Usuario
    if 'id_usuario_bdo' in df.columns:
        df = df.merge(dim_usuario, on='id_usuario_bdo', how='left')
        df['dk_usuario'] = df['dk_usuario'].astype('Int64')
        columns_to_drop.append('id_usuario_bdo')
    
    # Mensajero principal
    if 'id_mensajero_principal_bdo' in df.columns:
        df = df.merge(dim_mensajero.rename(columns={'dk_mensajero': 'dk_mensajero_principal'}),
                        left_on='id_mensajero_principal_bdo', right_on='id_mensajero_bdo', how='left')
        df['dk_mensajero_principal'] = df['dk_mensajero_principal'].astype('Int64')

        # Eliminar inmediatamente la columna de join para evitar conflictos
        df = df.drop(columns=['id_mensajero_bdo'])
        columns_to_drop.append('id_mensajero_principal_bdo')

    
    # Mensajero secundario
    if 'id_mensajero_secundario_bdo' in df.columns:
        df = df.merge(dim_mensajero.rename(columns={'dk_mensajero': 'dk_mensajero_secundario'}),
                        left_on='id_mensajero_secundario_bdo', right_on='id_mensajero_bdo', how='left')
        df['dk_mensajero_secundario'] = df['dk_mensajero_secundario'].astype('Int64')
        df = df.drop(columns=['id_mensajero_bdo'])
        columns_to_drop.append('id_mensajero_secundario_bdo')

    
    # Mensajero terciario
    if 'id_mensajero_terciario_bdo' in df.columns:
        df = df.merge(dim_mensajero.rename(columns={'dk_mensajero': 'dk_mensajero_terciario'}),
                        left_on='id_mensajero_terciario_bdo', right_on='id_mensajero_bdo', how='left')
        df['dk_mensajero_terciario'] = df['dk_mensajero_terciario'].astype('Int64')
        df = df.drop(columns=['id_mensajero_bdo'])
        columns_to_drop.append('id_mensajero_terciario_bdo')

    
    # Dirección origen - Agregar prefijo 'O-' para el mapeo
    if 'id_direccion_origen_bdo' in df.columns:
        df['temp_direccion_origen_key'] = 'O-' + df['id_direccion_origen_bdo'].astype(str)
        df = df.merge(dim_direccion.rename(columns={'dk_direccion': 'dk_direccion_origen'}), left_on='temp_direccion_origen_key',
                      right_on='id_direccion_bdo', how='left')
        df['dk_direccion_origen'] = df['dk_direccion_origen'].astype('Int64')
        df = df.drop(columns=['id_direccion_bdo'])  # eliminar inmediatamente
        columns_to_drop.extend(['id_direccion_origen_bdo', 'temp_direccion_origen_key'])

    
    if 'id_direccion_destino_bdo' in df.columns:
        df['temp_direccion_destino_key'] = 'D-' + df['id_direccion_destino_bdo'].astype(str)
        df = df.merge(dim_direccion.rename(columns={'dk_direccion': 'dk_direccion_destino'}), left_on='temp_direccion_destino_key',
                      right_on='id_direccion_bdo', how='left')
        df['dk_direccion_destino'] = df['dk_direccion_destino'].astype('Int64')
        df = df.drop(columns=['id_direccion_bdo'])  # eliminar inmediatamente
        columns_to_drop.extend(['id_direccion_destino_bdo', 'temp_direccion_destino_key'])

    # Tipo de pago
    if 'id_tipopago_bdo' in df.columns:
        df = df.merge(dim_tipopago, on='id_tipopago_bdo', how='left')
        df['dk_tipopago'] = df['dk_tipopago'].astype('Int64')
        columns_to_drop.append('id_tipopago_bdo')
    
    # Tipo de vehículo
    if 'id_tipovehiculo_bdo' in df.columns:
        df = df.merge(dim_tipovehiculo, on='id_tipovehiculo_bdo', how='left')
        df['dk_tipovehiculo'] = df['dk_tipovehiculo'].astype('Int64')
        columns_to_drop.append('id_tipovehiculo_bdo')
    
    # Eliminar las columnas originales para mantener el DataFrame limpio
    columns_to_drop = list(set(columns_to_drop))  # Eliminar duplicados
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
        print(f"Columnas eliminadas después del mapeo: {existing_columns_to_drop}")
    
    return df

def map_keys_for_fact_novedades (df, engine):
    """
    Mapea las business keys (id_*_bdo) a surrogate keys (dk_*) 
    y elimina las columnas originales para mantener el DataFrame limpio
    """
    logger.info("Ejecutando mapeo para fact_novedades")


    with engine.connect() as conn:
        # Cargar todas las dimensiones una sola vez
        fact_servicio = pd.read_sql("SELECT id_servicio_bdo, sk_servicio FROM fact_servicios", conn)
        dim_tiponovedad = pd.read_sql("SELECT id_tiponovedad_bdo, dk_tiponovedad FROM dimtiponovedad", conn)
        dim_mensajero = pd.read_sql("SELECT id_mensajero_bdo, dk_mensajero FROM dimmensajero", conn)

    # Lista para trackear las columnas que vamos a eliminar
    columns_to_drop = []

    # Servicio
    if 'id_servicio_bdo' in df.columns:
        df = df.merge(fact_servicio, on='id_servicio_bdo', how='left')
        df['sk_servicio'] = df['sk_servicio'].astype('Int64')
        columns_to_drop.append('id_servicio_bdo')
    
    #Tipo Novedad
    if 'id_tiponovedad_bdo' in df.columns:
        df = df.merge(dim_tiponovedad, on='id_tiponovedad_bdo', how='left')
        df['dk_tiponovedad'] = df['dk_tiponovedad'].astype('Int64')
        columns_to_drop.append('id_tiponovedad_bdo')
    
    #Mensajero
    if 'id_mensajero_bdo' in df.columns:
        df = df.merge(dim_mensajero, on='id_mensajero_bdo', how='left')
        df['dk_mensajero'] = df['dk_mensajero'].astype('Int64')
        columns_to_drop.append('id_mensajero_bdo')
    
    # Eliminar las columnas originales para mantener el DataFrame limpio
    columns_to_drop = list(set(columns_to_drop))  # Eliminar duplicados
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
        print(f"Columnas eliminadas después del mapeo: {existing_columns_to_drop}")
    
    return df

def map_keys_for_fact_estados_servicios (df, engine):
    """
    Mapea las business keys (id_*_bdo) a surrogate keys (dk_*) 
    y elimina las columnas originales para mantener el DataFrame limpio
    """

    logger.info("Ejecutando mapeo para fact_estados_servicios")


    with engine.connect() as conn:
        # Cargar todas las dimensiones una sola vez
        fact_servicio = pd.read_sql("SELECT id_servicio_bdo, sk_servicio FROM fact_servicios", conn)

    # Lista para trackear las columnas que vamos a eliminar
    columns_to_drop = []

    if 'id_servicio_bdo' in df.columns:
        df = df.merge(fact_servicio, on='id_servicio_bdo', how='left')
        df['sk_servicio'] = df['sk_servicio'].astype('Int64')
        columns_to_drop.append('id_servicio_bdo')
    
    # Eliminar las columnas originales para mantener el DataFrame limpio
    columns_to_drop = list(set(columns_to_drop))  # Eliminar duplicados
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    if existing_columns_to_drop:
        df = df.drop(columns=existing_columns_to_drop)
        print(f"Columnas eliminadas después del mapeo: {existing_columns_to_drop}")

    return df