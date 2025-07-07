import pandas as pd
from datetime import datetime, timedelta
import consultas

class KPICalculator:
    """
    Clase para calcular KPIs del dashboard de mensajería
    """
    
    def __init__(self):
        self.fecha_actual = datetime.now()
    
    def calcular_total_servicios(self, periodo='mes'):
        """
        Calcula el total de servicios en un período específico
        """
        try:
            # Opción 1: Usar datos reales de tus consultas
            df_servicios = consultas.servicios_por_mes()
            if df_servicios is not None and not df_servicios.empty:
                total = df_servicios['total_servicios'].sum()
                return f"{total:,}"
            else:
                return "0"
        except Exception as e:
            print(f"Error calculando total servicios: {e}")
            return "N/A"
    
    def calcular_mensajeros_activos(self):
        """
        Calcula el número de mensajeros activos
        """
        try:
            df_mensajeros = consultas.mensajeros_eficientes()
            if df_mensajeros is not None and not df_mensajeros.empty:
                # Contar mensajeros únicos
                total_mensajeros = len(df_mensajeros['id_mensajero_bdo'].unique())
                return str(total_mensajeros)
            else:
                return "0"
        except Exception as e:
            print(f"Error calculando mensajeros activos: {e}")
            return "N/A"
    
    def calcular_tiempo_promedio_entrega(self):
        """
        Calcula el tiempo promedio de entrega
        """
        try:
            df_tiempo = consultas.tiempo_promedio_entrega()
            if df_tiempo is not None and not df_tiempo.empty:
                tiempo = df_tiempo['tiempo_promedio_entrega_minutos'].iloc[0]
                return f"{tiempo:.0f} min"
            else:
                return "N/A"
        except Exception as e:
            print(f"Error calculando tiempo promedio: {e}")
            return "N/A"
    
    def calcular_eficiencia_general(self):
        """
        Calcula la eficiencia general del servicio
        """
        try:
            # Opción 1: Basado en servicios completados vs total
            df_servicios = consultas.servicios_por_mes()
            df_novedades = consultas.novedades_frecuentes()
            
            if df_servicios is not None and df_novedades is not None:
                total_servicios = df_servicios['total_servicios'].sum()
                total_novedades = df_novedades['frecuencia'].sum()
                
                # Eficiencia = (servicios sin novedad / total servicios) * 100
                eficiencia = ((total_servicios - total_novedades) / total_servicios) * 100
                return f"{eficiencia:.1f}%"
            else:
                return "N/A"
        except Exception as e:
            print(f"Error calculando eficiencia: {e}")
            return "N/A"
    
    def calcular_crecimiento_mensual(self):
        """
        Calcula el crecimiento comparado con el mes anterior
        """
        try:
            df_servicios = consultas.servicios_por_mes()
            if df_servicios is not None and len(df_servicios) >= 2:
                # Ordenar por mes
                df_servicios = df_servicios.sort_values('mes')
                ultimo_mes = df_servicios['total_servicios'].iloc[-1]
                mes_anterior = df_servicios['total_servicios'].iloc[-2]
                
                crecimiento = ((ultimo_mes - mes_anterior) / mes_anterior) * 100
                signo = "+" if crecimiento > 0 else ""
                return f"{signo}{crecimiento:.1f}%"
            else:
                return "N/A"
        except Exception as e:
            print(f"Error calculando crecimiento: {e}")
            return "N/A"
    
    def calcular_satisfaccion_cliente(self):
        """
        Calcula satisfacción basada en novedades
        """
        try:
            df_novedades = consultas.novedades_frecuentes()
            if df_novedades is not None and not df_novedades.empty:
                # Asumir que menos novedades = mayor satisfacción
                total_novedades = df_novedades['frecuencia'].sum()
                
                # Escala inversa: pocas novedades = alta satisfacción
                if total_novedades < 10:
                    satisfaccion = 95
                elif total_novedades < 50:
                    satisfaccion = 85
                elif total_novedades < 100:
                    satisfaccion = 75
                else:
                    satisfaccion = 65
                
                return f"{satisfaccion}%"
            else:
                return "N/A"
        except Exception as e:
            print(f"Error calculando satisfacción: {e}")
            return "N/A"
    
    def obtener_todos_los_kpis(self):
        """
        Obtiene todos los KPIs calculados
        """
        return {
            "total_servicios": self.calcular_total_servicios(),
            "mensajeros_activos": self.calcular_mensajeros_activos(),
            "tiempo_promedio": self.calcular_tiempo_promedio_entrega(),
            "eficiencia": self.calcular_eficiencia_general(),
            "crecimiento": self.calcular_crecimiento_mensual(),
            "satisfaccion": self.calcular_satisfaccion_cliente()
        }

# Función para integrar con el dashboard
def obtener_kpis_para_dashboard():
    """
    Función específica para el dashboard que retorna KPIs formateados
    """
    calculator = KPICalculator()
    kpis_data = calculator.obtener_todos_los_kpis()
    
    kpis_dashboard = [
        {
            "titulo": "Total Servicios",
            "valor": kpis_data["total_servicios"],
            "icono": "📦",
            "color": "#1f77b4",
            "descripcion": "Servicios procesados"
        },
        {
            "titulo": "Mensajeros Activos",
            "valor": kpis_data["mensajeros_activos"],
            "icono": "👥",
            "color": "#2ca02c",
            "descripcion": "Mensajeros en operación"
        },
        {
            "titulo": "Tiempo Promedio",
            "valor": kpis_data["tiempo_promedio"],
            "icono": "⏱️",
            "color": "#ffc107",
            "descripcion": "Tiempo de entrega"
        },
        {
            "titulo": "Eficiencia",
            "valor": kpis_data["eficiencia"],
            "icono": "📈",
            "color": "#17a2b8",
            "descripcion": "Servicios exitosos"
        },
        {
            "titulo": "Crecimiento",
            "valor": kpis_data["crecimiento"],
            "icono": "📊",
            "color": "#28a745",
            "descripcion": "Vs mes anterior"
        },
        {
            "titulo": "Satisfacción",
            "valor": kpis_data["satisfaccion"],
            "icono": "⭐",
            "color": "#fd7e14",
            "descripcion": "Nivel de servicio"
        }
    ]
    
    return kpis_dashboard

# Función alternativa con KPIs simulados (para testing)
def obtener_kpis_simulados():
    """
    KPIs simulados para testing cuando no hay datos reales
    """
    import random
    
    return [
        {
            "titulo": "Total Servicios",
            "valor": f"{random.randint(1000, 5000):,}",
            "icono": "📦",
            "color": "#1f77b4",
            "descripcion": "Servicios procesados"
        },
        {
            "titulo": "Mensajeros Activos",
            "valor": str(random.randint(20, 80)),
            "icono": "👥",
            "color": "#2ca02c",
            "descripcion": "Mensajeros en operación"
        },
        {
            "titulo": "Tiempo Promedio",
            "valor": f"{random.randint(15, 45)} min",
            "icono": "⏱️",
            "color": "#ffc107",
            "descripcion": "Tiempo de entrega"
        },
        {
            "titulo": "Eficiencia",
            "valor": f"{random.randint(85, 98)}%",
            "icono": "📈",
            "color": "#17a2b8",
            "descripcion": "Servicios exitosos"
        },
        {
            "titulo": "Crecimiento",
            "valor": f"{random.randint(-5, 25):+}%",
            "icono": "📊",
            "color": "#28a745",
            "descripcion": "Vs mes anterior"
        },
        {
            "titulo": "Satisfacción",
            "valor": f"{random.randint(80, 95)}%",
            "icono": "⭐",
            "color": "#fd7e14",
            "descripcion": "Nivel de servicio"
        }
    ]

# Ejemplo de uso
if __name__ == "__main__":
    # Crear instancia del calculador
    calculator = KPICalculator()
    
    # Obtener todos los KPIs
    kpis = calculator.obtener_todos_los_kpis()
    
    print("=== KPIs Calculados ===")
    for nombre, valor in kpis.items():
        print(f"{nombre}: {valor}")
    
    print("\n=== KPIs para Dashboard ===")
    kpis_dashboard = obtener_kpis_para_dashboard()
    for kpi in kpis_dashboard:
        print(f"{kpi['icono']} {kpi['titulo']}: {kpi['valor']}")