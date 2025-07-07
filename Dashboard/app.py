import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.graph_objects as go
import plotly.express as px
import consultas as consultas
from kpi_calculator import obtener_kpis_para_dashboard, obtener_kpis_simulados

# Configuración de colores profesionales
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e', 
    'success': '#2ca02c',
    'info': '#17a2b8',
    'warning': '#ffc107',
    'danger': '#dc3545',
    'dark': '#343a40',
    'light': '#f8f9fa',
    'background': '#ffffff',
    'sidebar': '#f8f9fa',
    'text': '#212529'
}

# Estilos para las pestañas (movido aquí para evitar errores)
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '12px 16px',
    'fontWeight': '500',
    'backgroundColor': '#f9f9f9',
    'color': COLORS['text'],
    'border': '1px solid #d6d6d6',
    'borderRadius': '8px 8px 0 0',
    'marginRight': '2px'
}

selected_tab_style = {
    'borderTop': f'3px solid {COLORS["primary"]}',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': 'white',
    'color': COLORS['primary'],
    'padding': '12px 16px',
    'fontWeight': '600',
    'border': '1px solid #d6d6d6',
    'borderRadius': '8px 8px 0 0'
}

app = dash.Dash(__name__)
app.title = "Dashboard de Servicios de Mensajería"

# Estilos CSS personalizados
app.layout = html.Div([
    # Header profesional
    html.Div([
        html.Div([
            html.H1("📦 Dashboard de Servicios de Mensajería", 
                   style={
                       "margin": "0",
                       "color": "white",
                       "fontSize": "2.5rem",
                       "fontWeight": "600",
                       "textAlign": "center"
                   }),
            html.P("Análisis integral de operaciones de mensajería",
                  style={
                      "margin": "10px 0 0 0",
                      "color": "rgba(255,255,255,0.8)",
                      "fontSize": "1.1rem",
                      "textAlign": "center"
                  })
        ], style={"padding": "30px"})
    ], style={
        "background": f"linear-gradient(135deg, {COLORS['primary']} 0%, {COLORS['secondary']} 100%)",
        "boxShadow": "0 4px 6px rgba(0,0,0,0.1)",
        "marginBottom": "20px"
    }),
    
    # Contenedor principal
    html.Div([
        # Panel de control
        html.Div([
            html.Div([
                html.H3("🔄 Panel de Control", style={
                    "color": COLORS['dark'],
                    "fontSize": "1.3rem",
                    "marginBottom": "15px",
                    "fontWeight": "600"
                }),
                html.Button([
                    html.I(className="fas fa-sync-alt", style={"marginRight": "8px"}),
                    "Actualizar Datos"
                ], id="btn-actualizar", n_clicks=0, style={
                    "backgroundColor": COLORS['success'],
                    "color": "white",
                    "border": "none",
                    "padding": "12px 24px",
                    "borderRadius": "8px",
                    "fontSize": "1rem",
                    "fontWeight": "500",
                    "cursor": "pointer",
                    "transition": "all 0.3s ease",
                    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                    "width": "100%"
                }),
                
                # Indicadores KPI
                html.Div(id="kpi-indicators", style={"marginTop": "20px"})
                
            ], style={
                "backgroundColor": COLORS['sidebar'],
                "padding": "25px",
                "borderRadius": "12px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.1)",
                "border": "1px solid rgba(0,0,0,0.05)"
            })
        ], style={"width": "25%", "display": "inline-block", "verticalAlign": "top"}),
        
        # Área de visualización
        html.Div([
            # Pestañas mejoradas
            dcc.Tabs(id="tabs", value="q1", children=[
                dcc.Tab(label="📊 Servicios por Mes", value="q1", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="📅 Servicios por Día", value="q2", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="⏰ Hora Pico", value="q3", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="👥 Cliente/Mes", value="q4", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="🏆 Mensajeros Top", value="q5", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="🏢 Sedes", value="q6", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="⏱️ Tiempo Entrega", value="q7", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="📈 Tiempos por Fase", value="q8", style=tab_style, selected_style=selected_tab_style),
                dcc.Tab(label="⚠️ Novedades", value="q9", style=tab_style, selected_style=selected_tab_style),
            ], style={"marginBottom": "20px"}),
            
            # Gráfico principal con loading
            dcc.Loading(
                dcc.Graph(id="grafico-consulta", style={"height": "600px"}),
                type="circle",
                color=COLORS['primary']
            ),
            
            # Tabla de datos
            html.Div([
                html.H4("📋 Datos Detallados", style={
                    "color": COLORS['dark'],
                    "fontSize": "1.2rem",
                    "marginBottom": "15px",
                    "fontWeight": "600"
                }),
                dash_table.DataTable(
                    id="tabla-datos",
                    style_cell={
                        'textAlign': 'left',
                        'padding': '12px',
                        'fontFamily': 'Arial, sans-serif',
                        'fontSize': '14px'
                    },
                    style_header={
                        'backgroundColor': COLORS['primary'],
                        'color': 'white',
                        'fontWeight': 'bold'
                    },
                    style_data={
                        'backgroundColor': COLORS['light'],
                        'color': COLORS['text']
                    },
                    style_data_conditional=[
                        {
                            'if': {'row_index': 'odd'},
                            'backgroundColor': 'white'
                        }
                    ]
                )
            ], style={
                "backgroundColor": "white",
                "padding": "20px",
                "borderRadius": "12px",
                "boxShadow": "0 2px 8px rgba(0,0,0,0.1)",
                "marginTop": "20px"
            })
            
        ], style={"width": "73%", "display": "inline-block", "marginLeft": "2%"})
        
    ], style={"padding": "20px", "backgroundColor": "#f5f5f5", "minHeight": "100vh"})
    
], style={"fontFamily": "Arial, sans-serif"})

# Los estilos ya están definidos arriba

# Callback para KPIs
@app.callback(
    Output("kpi-indicators", "children"),
    Input("btn-actualizar", "n_clicks"),
    prevent_initial_call=True
)
def actualizar_kpis(n):
    try:
        # Intentar obtener KPIs reales
        kpis = obtener_kpis_para_dashboard()
    except Exception as e:
        print(f"Error obteniendo KPIs reales: {e}")
        # Usar KPIs simulados como fallback
        kpis = obtener_kpis_simulados()
    
    return [
        html.Div([
            html.Div([
                html.Span(kpi["icono"], style={"fontSize": "1.5rem", "marginRight": "10px"}),
                html.Div([
                    html.H4(kpi["valor"], style={
                        "margin": "0", 
                        "color": kpi["color"], 
                        "fontSize": "1.4rem", 
                        "fontWeight": "700"
                    }),
                    html.P(kpi["titulo"], style={
                        "margin": "0", 
                        "color": COLORS['text'], 
                        "fontSize": "0.9rem"
                    }),
                    html.P(kpi["descripcion"], style={
                        "margin": "0", 
                        "color": "gray", 
                        "fontSize": "0.8rem",
                        "fontStyle": "italic"
                    })
                ], style={"flex": "1"})
            ], style={"display": "flex", "alignItems": "center"})
        ], style={
            "backgroundColor": "white",
            "padding": "15px",
            "borderRadius": "8px",
            "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
            "marginBottom": "10px",
            "border": f"1px solid {kpi['color']}",
            "borderLeft": f"4px solid {kpi['color']}",
            "transition": "transform 0.2s ease",
            "cursor": "pointer"
        }, className="kpi-card") for kpi in kpis
    ]

# Callback principal mejorado
@app.callback(
    [Output("grafico-consulta", "figure"),
     Output("tabla-datos", "data"),
     Output("tabla-datos", "columns")],
    [Input("btn-actualizar", "n_clicks"),
     Input("tabs", "value")],
    prevent_initial_call=True
)
def actualizar_visualizacion(n, tab):
    consulta_funciones = {
        "q1": consultas.servicios_por_mes,
        "q2": consultas.servicios_por_dia,
        "q3": consultas.hora_pico_mensajeros,
        "q4": consultas.servicios_por_cliente_mes,
        "q5": consultas.mensajeros_eficientes,
        "q6": consultas.sedes_por_cliente,
        "q7": consultas.tiempo_promedio_entrega,
        "q8": consultas.tiempos_por_fase,
        "q9": consultas.novedades_frecuentes,
    }

    df = consulta_funciones[tab]()

    if df is None or df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="⚠️ No hay datos disponibles",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            font=dict(size=20, color=COLORS['danger']),
            showarrow=False
        )
        fig.update_layout(
            title="Sin datos para mostrar",
            template="plotly_white"
        )
        return fig, [], []

    # Configuración base para todos los gráficos
    fig_layout = {
        "template": "plotly_white",
        "font": {"family": "Arial, sans-serif", "size": 12},
        "title": {"font": {"size": 18, "color": COLORS['dark']}, "x": 0.5},
        "margin": {"l": 60, "r": 60, "t": 80, "b": 60},
        "plot_bgcolor": "white",
        "paper_bgcolor": "white"
    }

    # Preparar datos para tabla
    table_data = df.to_dict('records')
    table_columns = [{"name": col, "id": col} for col in df.columns]

    if tab == "q1":
        df = df.sort_values(by="total_servicios", ascending=False)
        fig = px.bar(
            df, 
            x="mes", 
            y="total_servicios", 
            title="📊 Servicios por Mes - Vista Interactiva",
            labels={"mes": "Mes", "total_servicios": "Total Servicios"},
            text="total_servicios",
            color="total_servicios",
            color_continuous_scale="Blues"
        )
        fig.update_traces(textposition="outside", hovertemplate="<b>%{x}</b><br>Servicios: %{y}<extra></extra>")
        fig.update_layout(showlegend=False, **fig_layout)
    
    elif tab == "q2":
        df = df.sort_values(by="total_solicitudes", ascending=False)
        fig = px.bar(
            df,
            x="dia_semana",
            y="total_solicitudes",
            title="📅 Distribución de Solicitudes por Día de la Semana",
            labels={"dia_semana": "Día", "total_solicitudes": "Total Solicitudes"},
            text="total_solicitudes",
            color="total_solicitudes",
            color_continuous_scale="Viridis"
        )
        fig.update_traces(textposition="outside", hovertemplate="<b>%{x}</b><br>Solicitudes: %{y}<extra></extra>")
        fig.update_layout(showlegend=False, **fig_layout)

    elif tab == "q3":
        df = df.sort_values(by="servicios_en_hora", ascending=False)
        fig = px.line(
            df,
            x="hora",
            y="servicios_en_hora",
            title="⏰ Análisis de Horas Pico - Ocupación de Mensajeros",
            labels={"hora": "Hora del Día", "servicios_en_hora": "Servicios Activos"},
            markers=True,
            line_shape="spline"
        )
        fig.update_traces(
            line=dict(width=3, color=COLORS['primary']),
            marker=dict(size=8, color=COLORS['secondary']),
            hovertemplate="<b>Hora: %{x}:00</b><br>Servicios: %{y}<extra></extra>"
        )
        fig.update_layout(**fig_layout)

    elif tab == "q4":
        df = df.sort_values(by="total_servicios", ascending=False)
        fig = px.bar(
            df,
            x="nombre_cliente",
            y="total_servicios",
            color="mes",
            title="👥 Servicios por Cliente y Mes - Vista Comparativa",
            labels={"nombre_cliente": "Cliente", "total_servicios": "Total Servicios"},
            text="total_servicios",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            xaxis_tickangle=-45,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            **fig_layout
        )

    elif tab == "q5":
        df = df.sort_values(by="total_servicios", ascending=False)
        fig = px.bar(
            df.head(10),  # Top 10 mensajeros
            x="id_mensajero_bdo",
            y="total_servicios",
            title="🏆 Top 10 Mensajeros por Rendimiento",
            labels={"id_mensajero_bdo": "ID Mensajero", "total_servicios": "Servicios Realizados"},
            text="total_servicios",
            color="total_servicios",
            color_continuous_scale="RdYlGn"
        )
        fig.update_traces(textposition="outside", hovertemplate="<b>Mensajero: %{x}</b><br>Servicios: %{y}<extra></extra>")
        fig.update_layout(showlegend=False, **fig_layout)
    
    elif tab == "q6":
        df = df.sort_values(by="total_servicios", ascending=False)
        fig = px.sunburst(
            df,
            path=["nombre_cliente", "nombre_sede"],
            values="total_servicios",
            title="🏢 Distribución de Servicios por Cliente y Sede",
            color="total_servicios",
            color_continuous_scale="Plasma"
        )
        fig.update_layout(**fig_layout)
    
    elif tab == "q7":
        tiempo = df["tiempo_promedio_entrega_minutos"].iloc[0]
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=tiempo,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "⏱️ Tiempo Promedio de Entrega (minutos)"},
            gauge={
                'axis': {'range': [None, 60]},
                'bar': {'color': COLORS['primary']},
                'steps': [
                    {'range': [0, 20], 'color': COLORS['success']},
                    {'range': [20, 40], 'color': COLORS['warning']},
                    {'range': [40, 60], 'color': COLORS['danger']}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 45
                }
            }
        ))
        fig.update_layout(height=500, **fig_layout)

    elif tab == "q8":
        fig = px.bar(
            df,
            x="tiempo_promedio_minutos",
            y="estado_nombre",
            orientation="h",
            title="📈 Análisis de Tiempos por Fase del Servicio",
            labels={"estado_nombre": "Fase del Servicio", "tiempo_promedio_minutos": "Duración Promedio (min)"},
            text="tiempo_promedio_minutos",
            color="tiempo_promedio_minutos",
            color_continuous_scale="Turbo"
        )
        fig.update_layout(
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False,
            **fig_layout
        )
        fig.update_traces(textposition="outside", hovertemplate="<b>%{y}</b><br>Tiempo: %{x} min<extra></extra>")

    elif tab == "q9":
        fig = px.pie(
            df,
            values="frecuencia",
            names="nombre_tipo_novedad",
            title="⚠️ Distribución de Novedades por Tipo",
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            hovertemplate="<b>%{label}</b><br>Frecuencia: %{value}<br>Porcentaje: %{percent}<extra></extra>"
        )
        fig.update_layout(**fig_layout)

    return fig, table_data, table_columns

if __name__ == "__main__":
    app.run(debug=True)