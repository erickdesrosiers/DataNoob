import dash_table
from dash import Dash, html, dcc, callback, Output, Input, State
import dash_bootstrap_components as dbc
from datetime import date, datetime, timedelta
import pandas as pd
import mysql.connector
from dash.exceptions import PreventUpdate
from dotenv import load_dotenv
import os
import requests
load_dotenv()

#Cargamos las variables de entorno de notion
# Token y DB de Notion desde variables de entorno
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")

# Llamamos los headers
headers_notion = {
    "Authorization": "Bearer " + NOTION_TOKEN,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# Función para crear página en Notion
def create_page_notion(data: dict):
    """
    data: diccionario con las propiedades que coincidan con los nombres y tipos de columnas en Notion.
    Por ejemplo:
    data = {
        "Title": {"title": [{"text": {"content": "Mi título"}}]},
        "URL": {"rich_text": [{"text": {"content": "https://ejemplo.com"}}]},
        "Published": {"date": {"start": "2025-08-19T12:00:00+00:00"}}
    }
    """
    create_url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"database_id": DATABASE_ID},
        "properties": data
    }

    res = requests.post(create_url, headers=headers_notion, json=payload)

    if res.status_code != 200 and res.status_code != 201:
        print("Error al guardar en Notion:", res.json())
    return res
# Dentro de tu callback mostrar_resumen, después de guardar en DB MySQL
def guardar_en_notion(correo, peticion, verticales, sitios, ips, descripcion,
                      fecha_inicio, fecha_final, fecha_peticion):

    # Construir datos en el esquema de Notion
    data_notion = {
        "Correo": {"title": [{"text": {"content": correo}}]},
        "Tipo de Peticion": {"rich_text": [{"text": {"content": peticion}}]},
        "Verticales": {"rich_text": [{"text": {"content": ", ".join(verticales)}}]},
        "Sitios": {"rich_text": [{"text": {"content": ", ".join(sitios)}}]},
        "IPs": {"rich_text": [{"text": {"content": ", ".join(ips)}}]},
        "Fecha Inicio": {"date": {"start": fecha_inicio, "end": None}},
        "Fecha Peticion": {"date": {"start": fecha_peticion, "end": None}},
        "Fecha Final": {"date": {"start": fecha_final, "end": None}},
        "Descripcion": {"rich_text": [{"text": {"content": descripcion}}]},
        "Estado": {"status": {"name": "Sin empezar"}}
    }

    res = create_page_notion(data_notion)
    if res.status_code != 200 and res.status_code != 201:
        print("Error al guardar en Notion:", res.text)
    return res

def obtener_id_unico(cursor, tabla, campo, valor):
    cursor.execute(f"SELECT id_{tabla} FROM {tabla} WHERE {campo} = %s", (valor,))
    res = cursor.fetchone()
    if res:
        return res[0]
    else:
        cursor.execute(f"INSERT INTO {tabla} ({campo}) VALUES (%s)", (valor,))
        return cursor.lastrowid

def obtener_id_usuario(cursor, nombre_usuario):
    # Buscar id en Usuarios_unicos
    cursor.execute("SELECT id_usuarios_unicos FROM Usuarios_unicos WHERE nombre_usuarios_unicos = %s", (nombre_usuario,))
    res = cursor.fetchone()
    if res:
        id_unico = res[0]
    else:
        cursor.execute("INSERT INTO Usuarios_unicos (nombre_usuarios_unicos) VALUES (%s)", (nombre_usuario,))
        id_unico = cursor.lastrowid

    # Buscar id_usuario en Usuarios
    cursor.execute("SELECT id_usuarios FROM Usuarios WHERE id_usuarios_unicos = %s", (id_unico,))
    res = cursor.fetchone()
    if res:
        return res[0]
    else:
        # Insertar nuevo usuario con ese id_usuarios_unicos y nombre_usuarios igual a nombre_usuario
        cursor.execute("INSERT INTO Usuarios (id_usuarios_unicos, nombre_usuarios) VALUES (%s, %s)", (id_unico, nombre_usuario))
        return cursor.lastrowid

def guardar_peticion_db(correo, peticion, verticales, sitios, ips, descripcion, fecha_inicio, fecha_final, fecha_peticion):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT"))
        )
        cursor = conn.cursor()

        # Obtener id_usuario
        id_usuario = obtener_id_usuario(cursor, correo)

        # Obtener id_tipo_peticion
        cursor.execute("SELECT id_tipo_peticion FROM Tipo_peticion WHERE nombre_peticion = %s", (peticion,))
        res = cursor.fetchone()
        if not res:
            raise Exception(f"Tipo_peticion '{peticion}' no encontrado.")
        id_tipo_peticion = res[0]


        cursor.execute(
            """
            INSERT INTO Peticion (id_tipo_peticion, id_usuarios, Descripción, fecha_petición, fecha_inicio, fecha_final)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (id_tipo_peticion, id_usuario, descripcion, fecha_peticion, fecha_inicio, fecha_final)
        )
        conn.commit()
        id_peticion = cursor.lastrowid

        # Relación con sitios
        for sitio in sitios:
            cursor.execute("SELECT id FROM Sitios WHERE nombre_sitio = %s", (sitio,))
            res_sitio = cursor.fetchone()
            if res_sitio:
                id_sitio = res_sitio[0]
                cursor.execute("INSERT INTO Peticion_Sitios (id_peticion, id_sitios) VALUES (%s, %s)", (id_peticion, id_sitio))

        # Relación con múltiples IPs
        for ip in ips:
            cursor.execute("SELECT id_ip FROM IP WHERE nombre_ip = %s", (ip,))
            res_ip = cursor.fetchone()
            if res_ip:
                id_ip = res_ip[0]
                cursor.execute("INSERT INTO Peticion_IP (id_ip, id_peticion) VALUES (%s, %s)", (id_ip, id_peticion))

        conn.commit()
        cursor.close()
        conn.close()

        return id_peticion

    except Exception as e:
        print("Error al guardar en la base de datos:", e)
        return None


# Cargar los correos desde archivo Excel
slack_path = os.getenv("SLACK")
df_emails = pd.read_excel(slack_path) #Asegurarse de cambiar constantemente la hoja para actualizar los correos
EMAILS = df_emails['email'].dropna().unique().tolist()


tipos_peticion = ['Audiencia', 'Comparación', 'Competencia', 'Benchmark', 'Comscore', 'Histórico', 'Demográfico']
verticales = ['Cine/Series', 'Gaming', 'Tecnología', 'Food', 'Auto', 'Por sitios']

# Se asocian los sitios por vertical
sitios_por_vertical = {
    'Cine/Series': ['Cine MX', 'Cine CO'],
    'Gaming': ['Juegos LATAM'],
    'Tecnología': ['Tech MX', 'Tech CO', 'Tech AR'],
    'Food': ['Comida MX'],
    'Auto': ['Coche MX']
}

#Creamos esta línea de código para obtener todos los sitios
todos_sitios = sum(sitios_por_vertical.values(), [])

#Definimos las IP de los lugares donde suelen hacerse las peticiones
IPs = ["GLOBAL", "LATAM", "México", "Colombia", "Perú", "Argentina"
    , "Chile"]


def contar_peticiones_no_finalizadas():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT"))
        )
        cursor = conn.cursor()

        # Cuenta todas las peticiones cuyo estado no sea 'Finalizada' o que sea NULL
        cursor.execute("""
                       SELECT COUNT(*)
                       FROM Peticion
                       WHERE estado_petición IS NULL
                          OR estado_petición != 'Finalizada'
                       """)

        resultado = cursor.fetchone()
        cursor.close()
        conn.close()
        return resultado[0] if resultado else 0

    except Exception as e:
        print("Error al contar peticiones no finalizadas:", e)
        return 0


app = Dash(__name__, external_stylesheets=[dbc.themes.SUPERHERO])


def create_tab1_layout(form=None) -> dbc.Container:
    """Crea el layout para el Tab1"""
    return dbc.Container([
        dbc.Card([
            dbc.CardHeader(html.H4("Formulario de Solicitudes", className="text-center card-title mb-4")),
            dbc.CardBody([
                dbc.Alert(
                    [
                        html.I(className="bi bi-info-circle-fill me-2"),
                        html.Span("¿Qué tipo de reporte deberías solicitar?"),
                        html.Ul([
                            html.Li([
                                html.Strong("Audiencia: "),
                                "Reporte de afinidad de audiencia basado en los intereses de los usuarios dentro de nuestras verticales. ",
                                html.Span("Aplica por defecto para México. Para otros países o combinaciones de países, será necesario realizar una solicitud adicional.")
                            ]),
                            html.Li([
                                html.Strong("Comparación: "),
                                "Reporte de tráfico que permite analizar dos periodos distintos para identificar incrementos o disminuciones en el tráfico."
                            ]),
                            html.Li([
                                html.Strong("Competencia: "),
                                "Reporte de investigación con información básica sobre sitios competidores. ",
                                html.Span("Aplica únicamente para México.")
                            ]),
                            html.Li([
                                html.Strong("Benchmark: "),
                                "Reporte comparativo entre la competencia y nuestras marcas. ",
                                html.Span("Disponible solo para México.")
                            ]),
                            html.Li([
                                html.Strong("Comscore: "),
                                "Reporte que muestra el ranking de sitios competidores dentro de la industria correspondiente, según datos de Comscore.",
                                html.Span("Disponible para Colombia y México.")
                            ]),
                            html.Li([
                                html.Strong("Histórico: "),
                                "Reporte de tráfico con métricas como páginas vistas, usuarios y sesiones para un periodo determinado. ",
                                html.Span("Se pueden incluir otras métricas disponibles en Google Analytics, si así se requiere.")
                            ]),
                            html.Li([
                                html.Strong("Demográfico: "),
                                "Reporte que toma en consideración edad, género, top 5 intereses y top 5 estados o regiones."
                            ]),
                        ], style={"marginTop": "0.5rem", "paddingLeft": "1rem"})
                    ],
                    color="warning",
                    dismissable=False,
                    style={
                        "fontSize": "0.85rem",
                        "padding": "0.75rem 1.25rem",
                        "marginBottom": "1rem",
                        "backgroundColor": "#fff3cd",
                        "borderColor": "#ffeeba"
                    }
                ),

                html.Label("Tu correo institucional:"),
                dcc.Dropdown(
                    id="email",
                    placeholder="Escribe tu correo…",
                    options=[{"label": e, "value": e} for e in EMAILS],
                    searchable=True,
                    clearable=True,
                    style={
                        "borderRadius": "10px",
                        "border": "1px solid #ccc",
                        "padding": "10px",
                        "fontSize": "16px",
                        "boxShadow": "0 2px 8px rgba(0,0,0,0.1)",
                        "marginBottom": "20px"
                    }
                ),

                html.Label("Tipo de petición:"),
                dcc.Dropdown(
                    options=[{'label': i, 'value': i} for i in tipos_peticion],
                    id='dropdown-peticion',
                    placeholder="Selecciona tipo de petición"
                ),

                dbc.Alert(
                    [
                        html.I(className="bi bi-info-circle-fill me-2"),
                        html.Span("La temporalidad se establecerá con corte al mes anterior, excepto en los casos de Comparación e Histórico, donde las fechas podrán ser personalizadas.")
                    ],
                    color="info",
                    dismissable=False,
                    style={
                        "fontSize": "0.85rem",
                        "padding": "0.5rem 1rem",
                        "marginBottom": "0.5rem"
                    }
                ),

                html.Div(
                    id='contenedor-fechas',
                    children=[
                        html.Label("Selecciona un rango de fechas (mm/aaaa):", className="mb-2"),
                        dbc.InputGroup([
                            dbc.InputGroupText("Inicio"),
                            dbc.Input(id='input-fecha-inicio', placeholder="mm/aaaa", type='text'),
                            dbc.InputGroupText("Fin"),
                            dbc.Input(id='input-fecha-fin', placeholder="mm/aaaa", type='text'),
                        ], className="mb-4")
                    ],
                    hidden=True
                ),

                html.Div(
                    id='contenedor-historico',
                    children=[
                        html.Label("Selecciona un rango de fechas:", className="mb-2"),
                        dcc.DatePickerRange(
                            id='my-date-picker-range',
                            min_date_allowed=date(2024, 1, 1),
                            max_date_allowed=date.today(),
                            initial_visible_month=date.today(),
                            end_date=date.today()
                        )
                    ],
                    hidden=True
                ),

                dbc.Alert(
                    [
                        html.I(className="bi bi-info-circle-fill me-2"),
                        html.Span("Al seleccionar la opción <<por sitios>> en Vertical podrás pedir cualquier sitio en tu petición independientemente de la vertical.")
                    ],
                    color="info",
                    dismissable=False,
                    style={
                        "fontSize": "0.85rem",
                        "padding": "0.5rem 1rem",
                        "marginBottom": "0.5rem"
                    }
                ),

                html.Label("Vertical:"),
                dcc.Dropdown(
                    options=[{'label': i, 'value': i} for i in verticales],
                    id='dropdown-vertical',
                    placeholder="Selecciona vertical(es)",
                    multi=True
                ),

                html.Br(),

                html.Label("Sitio:"),
                dcc.Dropdown(
                    id='dropdown-sitio',
                    placeholder="Selecciona sitio(s)",
                    multi=True
                ),

                html.Br(),

                html.Label("Región IP:"),
                dcc.Dropdown(
                    options=[{'label': i, 'value': i} for i in IPs],
                    id='dropdown-ip',
                    placeholder="Selecciona región IP",
                    multi=True
                ),

                html.Div(id='ip-warning', style={'color': 'red', 'marginTop': '10px'}),

                html.Br(),

                html.Label("Descripción:"),
                dcc.Textarea(
                    id='input-descripcion',
                    placeholder='Escribe una descripción de tu solicitud...',
                    style={'width': '100%', 'height': 100}
                ),

                html.Br(), html.Br(),

                dbc.Button('Enviar solicitud', id='boton-enviar', n_clicks=0,
                           style={
                               'backgroundColor': '#00ffea',
                               'color': '#1e1e2f',
                               'fontWeight': 'bold',
                               'border': '2px solid #00ffea',
                               'borderRadius': '5px',
                               'boxShadow': '0 0 10px #00ffea'
                           })
                ,

                html.Div(id='output-resumen', style={'marginTop': '20px', 'fontWeight': 'bold'}),

                html.Br(),

                # Tarjeta moderna con estado de solicitud
                html.Div(
                    id='contador-finalizadas',
                    style={
                        'fontWeight': 'bold',
                        'fontSize': '16px',
                        'padding': '12px 16px',
                        'backgroundColor': '#e3f2fd',  # azul claro de fondo
                        'border': '1px solid #90caf9',  # azul suave
                        'borderRadius': '8px',
                        'color': '#0d3b66',  # azul marino para texto
                        'marginTop': '25px',
                        'boxShadow': '0 1px 3px rgba(0,0,0,0.1)'
                    }
                )

            ])]),
        html.Br(),
        html.Hr(),
        html.H5("Peticiones en espera:", style={"marginTop": "20px"}),
        dcc.Loading(
            id="loading-tabla-espera",
            children=[
                html.Div(id="tabla-espera")
            ],
            type="default"
        ),

    ], fluid=True)

def obtener_peticiones_en_espera():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            port=int(os.getenv("DB_PORT"))
        )
        query = """
        SELECT
            uu.nombre_usuarios_unicos AS usuario,
            tp.nombre_peticion AS tipo_peticion,
            p.fecha_petición,
            p.Descripción,
            p.estado_petición
        FROM Peticion p
        JOIN Usuarios u ON p.id_usuarios = u.id_usuarios
        JOIN Usuarios_unicos uu ON u.id_usuarios_unicos = uu.id_usuarios_unicos
        JOIN Tipo_peticion tp ON p.id_tipo_peticion = tp.id_tipo_peticion
        JOIN Peticion_Sitios ps ON p.id_peticion = ps.id_peticion
        JOIN Sitios s ON ps.id_sitios = s.id
        JOIN Verticales v ON s.id_vertical = v.id
        JOIN Peticion_IP pip ON p.id_peticion = pip.id_peticion
        JOIN IP ip ON pip.id_ip = ip.id_ip
        WHERE estado_petición IS NULL OR estado_petición != 'Finalizada'
        ORDER BY p.fecha_petición DESC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        print("Error al obtener las peticiones:", e)
        return pd.DataFrame()




def register_tabform_callbacks(app):
    @app.callback(
        Output('tabla-espera', 'children'),
        Input('boton-enviar', 'n_clicks'),
        prevent_initial_call=True
    )
    def mostrar_tabla_espera(n_clicks):
        df = obtener_peticiones_en_espera()
        if df.empty:
            return dbc.Alert("No hay peticiones en espera.", color="secondary")

        return dash_table.DataTable(
            columns=[{"name": i, "id": i} for i in df.columns],
            data=df.to_dict('records'),
            #page_size=50,
            style_table={'overflowX': 'auto'},
            style_cell={
                'textAlign': 'left',
                'fontFamily': 'Arial',
                'padding': '5px',
                'minWidth': '100px',
                'whiteSpace': 'normal'
            },
            style_header={
                'backgroundColor': '#f2f2f2',
                'fontWeight': 'bold'
            },
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': '#fafafa'
                }
            ]
        )

    @app.callback(
        Output('contenedor-fechas', 'hidden'),
        Output('contenedor-historico', 'hidden'),
        Input('dropdown-peticion', 'value')
    )
    def mostrar_ocultar_fechas(peticion):
        return (
            not (peticion == 'Comparación'),
            not (peticion in ['Histórico', 'Demográfico'])
        )

    @app.callback(
        Output('dropdown-sitio', 'options'),
        Output('dropdown-sitio', 'value'),
        Output('dropdown-sitio', 'disabled'),
        Output('dropdown-ip', 'options'),
        Input('dropdown-vertical', 'value'),
        Input('dropdown-peticion', 'value')
    )
    # Esta función actualiza los sitios por petición, recordemos que  para Benchmark, Competencia solo podemos incluir sitios de MX y 3D Juegos e IP MX.
    # Mientras Comscore solo para México y Colombia. El resto de peticiones funcionan normal.



    def actualizar_sitios_por_vertical(verticales_sel, peticion):
        opciones_ip = [{'label': i, 'value': i} for i in IPs]  # default
        sitios_set = set()
        sitios_mx = [s for s in todos_sitios if 'MX' in s]
        sitios_co = [s for s in todos_sitios if 'CO' in s]

        if peticion in ['Benchmark', 'Competencia']:
            opciones_ip = [{'label': 'México', 'value': 'México'}]
            if verticales_sel and 'Por sitios' in verticales_sel:
                sitios = sitios_mx + ['Juegos LATAM']  # Aquí se agrega
                opciones = [{'label': sitio, 'value': sitio} for sitio in sitios]
                return opciones, [], False, opciones_ip
            else:
                for v in verticales_sel or []:
                    if v == 'Gaming':
                        sitios_set.add('Juegos LATAM')
                    sitios_set.update([s for s in sitios_por_vertical.get(v, []) if 'MX' in s])
                sitios = list(sitios_set)
                opciones = [{'label': sitio, 'value': sitio} for sitio in sitios]
                return opciones, sitios, True, opciones_ip

        elif peticion == 'Comscore':
            opciones_ip = [{'label': i, 'value': i} for i in ['México', 'Colombia']]
            if verticales_sel and 'Por sitios' in verticales_sel:
                sitios = sitios_mx + sitios_co + ['Juegos LATAM']  # Aquí también se agrega
                opciones = [{'label': sitio, 'value': sitio} for sitio in sitios]
                return opciones, [], False, opciones_ip
            else:
                for v in verticales_sel or []:
                    if v == 'Gaming':
                        sitios_set.add('Juegos LATAM')
                    sitios_set.update([s for s in sitios_por_vertical.get(v, []) if 'MX' in s or 'CO' in s])
                sitios = list(sitios_set)
                opciones = [{'label': sitio, 'value': sitio} for sitio in sitios]
                return opciones, sitios, True, opciones_ip

        else:
            if not verticales_sel or 'Por sitios' in verticales_sel:
                sitios = todos_sitios
                opciones = [{'label': sitio, 'value': sitio} for sitio in sitios]
                return opciones, [], False, opciones_ip
            else:
                for v in verticales_sel:
                    if v == 'Gaming':
                        sitios_set.add('Juegos LATAM')
                    sitios_set.update(sitios_por_vertical.get(v, []))
                sitios = list(sitios_set)
                opciones = [{'label': sitio, 'value': sitio} for sitio in sitios]
                return opciones, sitios, True, opciones_ip

    # Callback: validar IPs
    @app.callback(
        Output('dropdown-ip', 'value'),
        Output('ip-warning', 'children'),
        Input('dropdown-ip', 'value')
    )
    # Esta función nos permite seleccionar únicamente latam o global como IP (también son excluyentes) y que no se pueda combinar con otras IPS
    def validar_ips(ips_seleccionadas):
        if not ips_seleccionadas:
            return [], ''

        exclusivos = {'GLOBAL', 'LATAM'}
        seleccionados_exclusivos = [ip for ip in ips_seleccionadas if ip in exclusivos]
        seleccionados_otros = [ip for ip in ips_seleccionadas if ip not in exclusivos]

        # Caso 1: mezclan GLOBAL o LATAM con otras regiones
        if seleccionados_exclusivos and seleccionados_otros:
            return seleccionados_exclusivos, "No puedes seleccionar otras regiones junto con GLOBAL o LATAM."

        # Caso 2: seleccionan GLOBAL y LATAM juntos
        if len(seleccionados_exclusivos) > 1:
            return [], "No puedes seleccionar GLOBAL y LATAM al mismo tiempo."

        return ips_seleccionadas, ''

    # Callback: mostrar resumen
    @callback(
        Output('output-resumen', 'children'),
        Input('boton-enviar', 'n_clicks'),
        State('email', 'value'),  # 1. correo
        State('dropdown-peticion', 'value'),  # 2. peticion
        State('dropdown-vertical', 'value'),  # 3. verticales
        State('dropdown-sitio', 'value'),  # 4. sitios
        State('dropdown-ip', 'value'),  # 5. ips
        State('input-descripcion', 'value'),  # 6. descripcion
        State('input-fecha-inicio', 'value'),  # 7. fecha_inicio
        State('input-fecha-fin', 'value'),  # 8. fecha_fin
        State('my-date-picker-range', 'start_date'),  # 9. fecha_hist_inicio
        State('my-date-picker-range', 'end_date')  # 10. fecha_hist_fin
    )
    def mostrar_resumen(n_clicks, correo, peticion, verticales, sitios, ips, descripcion,
                        fecha_inicio, fecha_fin, fecha_hist_inicio, fecha_hist_fin):

        if n_clicks == 0:
            return ''

        if not peticion or not verticales or not sitios or not ips or not descripcion or not correo:
            ejemplo = (
                "- Correo: xxxx@webedia-group.com\n"
                "- Tipo de petición: Audiencia\n"
                "- Vertical: Tecnología, Gaming\n"
                "- Sitio: Tech MX, Tech CO, Tech AR, Juegos LATAM\n"
                "- Región IP: México, Colombia \n"
                "- Fecha de inicio: 2025-05-01"
                "- Fecha final: 2025-05-31"
                "- Descripción: Necesitamos insights de audiencias interesadas en gaming y tecnología para validar afinidad con X marca enfocada en jóvenes de 18+ en México y Colombia."
            )
            return dbc.Alert(
                [
                    html.I(className="bi bi-exclamation-triangle-fill me-2"),
                    html.Span("Por favor completa todos los campos del formulario."),
                    html.Br(), html.Br(),
                    html.Strong("Ejemplo de llenado correcto:"),
                    html.Pre(ejemplo, style={"whiteSpace": "pre-wrap", "marginTop": "0.5rem"})
                ],
                color="danger",
                dismissable=True
            )

        # Inicializar fechas
        fecha_inicio_resumen = ""
        fecha_fin_resumen = ""

        if peticion in ['Histórico', 'Demográfico']:
            if not fecha_hist_inicio or not fecha_hist_fin:
                return dbc.Alert(
                    [
                        html.I(className="bi bi-exclamation-triangle-fill me-2"),
                        html.Span("Debes seleccionar un rango de fechas para la petición.")
                    ],
                    color="danger",
                    dismissable=True
                )
            try:
                fecha_hist_inicio_dt = datetime.strptime(fecha_hist_inicio, "%Y-%m-%d")
                fecha_hist_fin_dt = datetime.strptime(fecha_hist_fin, "%Y-%m-%d")
                fecha_inicio_resumen = fecha_hist_inicio_dt.strftime('%Y-%m-%d')
                fecha_fin_resumen = fecha_hist_fin_dt.strftime('%Y-%m-%d')
            except Exception:
                return dbc.Alert(
                    [
                        html.I(className="bi bi-exclamation-triangle-fill me-2"),
                        html.Span("Error al interpretar las fechas seleccionadas.")
                    ],
                    color="danger",
                    dismissable=True
                )

        elif peticion == 'Comparación':
            if not fecha_inicio or not fecha_fin:
                return dbc.Alert(
                    [
                        html.I(className="bi bi-exclamation-triangle-fill me-2"),
                        html.Span("Debes ingresar ambas fechas en formato mm/aaaa.")
                    ],
                    color="danger",
                    dismissable=True
                )
            try:
                fecha_inicio_dt = datetime.strptime(fecha_inicio, "%m/%Y")
                fecha_fin_dt = datetime.strptime(fecha_fin, "%m/%Y")
                fecha_min = datetime.strptime("01/2024", "%m/%Y")
                hoy = datetime.today()
                fecha_max = datetime(hoy.year, hoy.month, 1) - timedelta(days=1)

                if fecha_inicio_dt < fecha_min or fecha_fin_dt < fecha_min:
                    return dbc.Alert(
                        [
                            html.I(className="bi bi-exclamation-triangle-fill me-2"),
                            html.Span("Las fechas no pueden ser anteriores a enero de 2024.")
                        ],
                        color="danger",
                        dismissable=True
                    )

                if fecha_inicio_dt > fecha_max or fecha_fin_dt > fecha_max:
                    return dbc.Alert(
                        [
                            html.I(className="bi bi-exclamation-triangle-fill me-2"),
                            html.Span(f"Las fechas no pueden ser posteriores a {fecha_max.strftime('%Y/%m')}.")
                        ],
                        color="danger",
                        dismissable=True
                    )

                fecha_inicio_resumen = fecha_inicio_dt.strftime('%Y-%m-01')
                if fecha_fin_dt.month == 12:
                    siguiente_mes = datetime(fecha_fin_dt.year + 1, 1, 1)
                else:
                    siguiente_mes = datetime(fecha_fin_dt.year, fecha_fin_dt.month + 1, 1)
                fecha_fin_dt = siguiente_mes - timedelta(days=1)
                fecha_fin_resumen = fecha_fin_dt.strftime('%Y-%m-%d')

            except ValueError:
                return dbc.Alert(
                    [
                        html.I(className="bi bi-exclamation-triangle-fill me-2"),
                        html.Span("Formato inválido. Usa mm/aaaa para ambas fechas.")
                    ],
                    color="danger",
                    dismissable=True
                )

        else:
            hoy = datetime.today()
            primer_dia_mes_actual = datetime(hoy.year, hoy.month, 1)
            ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
            primer_dia_mes_anterior = datetime(ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month, 1)
            fecha_inicio_resumen = primer_dia_mes_anterior.strftime('%Y-%m-%d')
            fecha_fin_resumen = ultimo_dia_mes_anterior.strftime('%Y-%m-%d')

        # Fecha actual de la petición (nueva línea)
        fecha_peticion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        guardar_peticion_db(
            correo=correo,
            peticion=peticion,
            verticales=verticales,
            sitios=sitios,
            ips=ips,
            descripcion=descripcion,
            fecha_inicio=fecha_inicio_resumen,
            fecha_final=fecha_fin_resumen,
            fecha_peticion=fecha_peticion
        )

        guardar_en_notion(
            correo=correo,
            peticion=peticion,
            verticales=verticales,
            sitios=sitios,
            ips=ips,
            descripcion=descripcion,
            fecha_inicio=fecha_inicio_resumen,
            fecha_final=fecha_fin_resumen,
            fecha_peticion=fecha_peticion
        )

        return dbc.Alert(
            [
                html.I(className="bi bi-check-circle-fill me-2"),
                html.Span("Resumen de la solicitud:"),
                html.Ul([
                    html.Li(f"Correo: {correo}"),
                    html.Li(f"Tipo de petición: {peticion}"),
                    html.Li(f"Vertical(es): {', '.join(verticales)}"),
                    html.Li(f"Sitio(s): {', '.join(sitios)}"),
                    html.Li(f"Región IP: {', '.join(ips)}"),
                    html.Li(f"Fecha de inicio: {fecha_inicio_resumen}"),
                    html.Li(f"Fecha final: {fecha_fin_resumen}"),
                    html.Li(f"Fecha de la petición: {fecha_peticion}"),
                    html.Li(f"Descripción: {descripcion}")
                ])
            ],
            color="success",
            dismissable=True
        )

    @app.callback(
        Output('contador-finalizadas', 'children'),
        Input('boton-enviar', 'n_clicks'),
        prevent_initial_call=True
    )
    def actualizar_contador_no_finalizadas(n_clicks):
        if not n_clicks:
            raise PreventUpdate

        en_espera = contar_peticiones_no_finalizadas()
        return f"Tu solicitud está detrás de {en_espera} peticiones aún no finalizadas. Mira la tabla debajo para ver de cuáles se tratan."


app.layout = create_tab1_layout()

if __name__ == '__main__':
    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    app.layout = create_tab1_layout()
    register_tabform_callbacks(app)
    app.run(debug=True, port=8051)