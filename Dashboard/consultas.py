import pandas as pd
from db_config import obtener_conexion


#Consulta 1: Meses con más servicios solicitados
def servicios_por_mes():
    query = """
    SELECT 
        t.mes, 
        COUNT(*) AS total_servicios
    FROM fact_servicios s
    JOIN dimtiempo t ON s.id_tiempo_solicitud = t.id_fecha_completa
    GROUP BY t.mes
    ORDER BY total_servicios DESC;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Consulta 2: Días con más solicitudes (día de la semana)
def servicios_por_dia():
    query = """
    SELECT 
        t.dia_semana, 
        COUNT(*) AS total_solicitudes
    FROM fact_servicios s
    JOIN dimtiempo t ON s.id_tiempo_solicitud = t.id_fecha_completa
    GROUP BY t.dia_semana
    ORDER BY total_solicitudes DESC;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#Consulta 3: Horas en que los mensajeros están más ocupados
def hora_pico_mensajeros():
    query = """
    SELECT 
        t.hora, 
        COUNT(*) AS servicios_en_hora
    FROM fact_servicios s
    JOIN dimtiempo t ON s.id_tiempo_solicitud = t.id_fecha_completa
    GROUP BY t.hora
    ORDER BY servicios_en_hora DESC;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Consulta 4: Servicios por cliente y por mes
def servicios_por_cliente_mes():
    query = """
    SELECT 
        c.nombre_cliente, 
        t.mes, 
        COUNT(*) AS total_servicios
    FROM fact_servicios s
    JOIN dimcliente c ON s.dk_cliente = c.dk_cliente
    JOIN dimtiempo t ON s.id_tiempo_solicitud = t.id_fecha_completa
    GROUP BY c.nombre_cliente, t.mes
    ORDER BY c.nombre_cliente, t.mes;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#Consulta 5: Mensajeros más eficientes (más servicios prestados)
def mensajeros_eficientes():
    query = """
    SELECT 
        m.id_mensajero_bdo, 
        COUNT(*) AS total_servicios
    FROM fact_servicios s
    JOIN dimmensajero m ON s.dk_mensajero_principal = m.dk_mensajero
    WHERE s.flag_completado = TRUE
    GROUP BY m.id_mensajero_bdo
    ORDER BY total_servicios DESC
    LIMIT 15;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#Consulta 6: Sedes que más solicitan servicios por cliente
def sedes_por_cliente():
    query = """
    SELECT 
        c.nombre_cliente, 
        u.nombre_sede, 
        COUNT(*) AS total_servicios
    FROM fact_servicios s
    JOIN dimusuario u ON s.dk_usuario = u.dk_usuario
    JOIN dimcliente c ON u.id_cliente_bdo = c.id_cliente_bdo
    GROUP BY c.nombre_cliente, u.nombre_sede
    ORDER BY c.nombre_cliente, total_servicios DESC;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#Consulta 7: Tiempo promedio de entrega (servicios completados)
def tiempo_promedio_entrega():
    query = """
    SELECT 
        ROUND(AVG(tiempo_total_servicio_minutos), 2) AS tiempo_promedio_entrega_minutos
    FROM fact_servicios
    WHERE flag_completado = TRUE;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#Consulta 8: Tiempos de espera por cada fase del servicio
def tiempos_por_fase():
    query = """
    SELECT
        estado_nombre,
        ROUND(AVG(duracion_estado_minutos),2) AS tiempo_promedio_minutos
    FROM fact_estados_servicio
    GROUP BY estado_nombre
    ORDER BY tiempo_promedio_minutos DESC;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

#Consulta 9: Novedades más frecuentes
def novedades_frecuentes():
    query = """
    SELECT 
        nombre_tipo_novedad, 
        COUNT(*) AS frecuencia
    FROM fact_novedades
    GROUP BY nombre_tipo_novedad
    ORDER BY frecuencia DESC;
    """
    conn = obtener_conexion()
    df = pd.read_sql(query, conn)
    conn.close()
    return df