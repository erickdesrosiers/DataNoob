import pandas as pd
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import os
from dotenv import load_dotenv

load_dotenv()

# Cargar dataset
df = pd.read_excel("Exámen.xlsx", sheet_name="Dataset")

# Inicializar app
app = Dash(__name__)

# Layout del dashboard
app.layout = html.Div([
    html.H1("Dashboard de Consumo de Contenido", style={"textAlign": "center"}),

    # Filtro por región
    html.Label("Selecciona Región:"),
    dcc.Dropdown(
        id="region_filter",
        options=[{"label": r, "value": r} for r in df["REGION"].unique()],
        multi=True,
        value=[]
    ),

    html.Br(),

    # KPIs
    html.Div([
        html.Div(id="kpi_clients", style={"display":"inline-block", "width":"24%", "textAlign":"center"}),
        html.Div(id="kpi_top_genre", style={"display":"inline-block", "width":"24%", "textAlign":"center"}),
        html.Div(id="kpi_multi_device", style={"display":"inline-block", "width":"24%", "textAlign":"center"}),
    ]),

    html.Br(),

    # Gráficos
    dcc.Graph(id="genre_chart"),
    dcc.Graph(id="device_chart"),
    dcc.Graph(id="time_series"),
    dcc.Graph(id="region_chart"),
    dcc.Graph(id="top_content_chart"),
    dcc.Graph(id="recurrence_chart"),
    dcc.Graph(id="region_genre_heatmap")
])

# Callback para actualizar gráficos y KPIs
@app.callback(
    [Output("genre_chart", "figure"),
     Output("device_chart", "figure"),
     Output("time_series", "figure"),
     Output("region_chart", "figure"),
     Output("top_content_chart", "figure"),
     Output("recurrence_chart", "figure"),
     Output("region_genre_heatmap", "figure"),
     Output("kpi_clients", "children"),
     Output("kpi_top_genre", "children"),
     Output("kpi_multi_device", "children")],
    [Input("region_filter", "value")]
)
def update_dashboard(selected_regions):
    dff = df.copy()
    if selected_regions:
        dff = dff[dff["REGION"].isin(selected_regions)]

    # KPI 1: Clientes que consumen video
    num_clients = dff["CUSTOMER_ID"].nunique()
    kpi_clients = html.Div([html.H3("Clientes que consumen video"), html.H1(f"{num_clients}")])

    # KPI 2: Género más visto
    top_genre = dff.groupby("GENRE")["SCREENTIME"].sum().idxmax()
    kpi_top_genre = html.Div([html.H3("Género más visto"), html.H1(f"{top_genre}")])

    # KPI 3: Usuarios multi-dispositivo
    device_count = dff.groupby("CUSTOMER_ID")["DEVICE"].nunique()
    multi_device_pct = (device_count > 1).mean() * 100
    kpi_multi_device = html.Div([html.H3("Usuarios multi-dispositivo"), html.H1(f"{multi_device_pct:.1f}%")])

    # Gráfico 1: Tiempo de pantalla por género
    fig1 = px.bar(dff.groupby("GENRE", as_index=False)["SCREENTIME"].sum(),
                  x="GENRE", y="SCREENTIME",
                  title="Tiempo de pantalla por género")

    # Gráfico 2: Distribución de dispositivos
    fig2 = px.pie(dff, names="DEVICE", title="Distribución de dispositivos")

    # Gráfico 3: Evolución del consumo en el tiempo
    dff_time = dff.groupby("DATE", as_index=False)["SCREENTIME"].sum()
    fig3 = px.line(dff_time, x="DATE", y="SCREENTIME", title="Evolución del consumo")

    # Gráfico 4: Consumo por región
    fig4 = px.bar(dff.groupby("REGION", as_index=False)["SCREENTIME"].sum(),
                  x="REGION", y="SCREENTIME", title="Consumo por región")

    # Gráfico 5: Top 10 contenido más visto
    top_content = dff.groupby("TITLE")["SCREENTIME"].sum().sort_values(ascending=False).head(10).reset_index()
    fig5 = px.bar(top_content, x="TITLE", y="SCREENTIME", title="Top 10 contenido más visto")

    # Gráfico 6: Recurrencia de consumo por cliente
    consumption_counts = dff.groupby("CUSTOMER_ID").size().reset_index(name="count")
    fig6 = px.histogram(consumption_counts, x="count", nbins=20, title="Recurrencia de consumo por cliente")

    # Gráfico 7: Heatmap de región vs género
    region_genre = dff.groupby(["REGION","GENRE"])["SCREENTIME"].sum().reset_index()
    fig7 = px.density_heatmap(region_genre, x="REGION", y="GENRE", z="SCREENTIME",
                              title="Relación entre región y género")

    return fig1, fig2, fig3, fig4, fig5, fig6, fig7, kpi_clients, kpi_top_genre, kpi_multi_device

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8053, debug=True)

