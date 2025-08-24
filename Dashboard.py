import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import os
import requests
from dotenv import load_dotenv
load_dotenv()

# Cargar dataset
df = pd.read_excel(os.getenv("EXAMEN"), sheet_name="Dataset")

# Inicializar app
app = Dash(__name__)

# Layout del dashboard
app.layout = html.Div([
    html.H1("Dashboard de Consumo de Contenido"),

    # Filtro por región
    html.Label("Selecciona Región:"),
    dcc.Dropdown(
        id="region_filter",
        options=[{"label": r, "value": r} for r in df["REGION"].unique()],
        multi=True,
        value=[]
    ),

    # Gráfico 1: Tiempo de pantalla por género
    dcc.Graph(id="genre_chart"),

    # Gráfico 2: Dispositivos más usados
    dcc.Graph(id="device_chart"),

    # Gráfico 3: Evolución del consumo en el tiempo
    dcc.Graph(id="time_series")
])

# Callbacks para actualizar gráficos
@app.callback(
    [Output("genre_chart", "figure"),
     Output("device_chart", "figure"),
     Output("time_series", "figure")],
    [Input("region_filter", "value")]
)
def update_charts(selected_regions):
    dff = df.copy()
    if selected_regions:
        dff = dff[dff["REGION"].isin(selected_regions)]

    # Gráfico 1: Screentime por género
    fig1 = px.bar(dff.groupby("GENRE", as_index=False)["SCREENTIME"].sum(),
                  x="GENRE", y="SCREENTIME",
                  title="Tiempo de pantalla por género")

    # Gráfico 2: Dispositivos más usados
    fig2 = px.pie(dff, names="DEVICE", title="Distribución de dispositivos")

    # Gráfico 3: Consumo en el tiempo
    dff_time = dff.groupby("DATE", as_index=False)["SCREENTIME"].sum()
    fig3 = px.line(dff_time, x="DATE", y="SCREENTIME", title="Evolución del consumo")

    return fig1, fig2, fig3


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8053)

