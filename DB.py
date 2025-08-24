import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# Título de la app
st.set_page_config(page_title="Dashboard de Consumo de Contenido", layout="wide")
st.title("Dashboard de Consumo de Contenido")

# Cargar dataset
df = pd.read_excel("DATA/Examen.xlsx", sheet_name="Dataset")
#un cambio jsjsj
# Normalizar columnas
df.columns = df.columns.str.strip().str.upper().str.replace(" ", "_")

# Filtro por región
regiones = df["REGION"].unique()
selected_regions = st.multiselect("Selecciona Región:", options=regiones, default=list(regiones))

if selected_regions:
    dff = df[df["REGION"].isin(selected_regions)]
else:
    dff = df.copy()

# ----------------- KPIs -----------------
col1, col2, col3 = st.columns(3)

# KPI 1: Clientes que consumen video
num_clients = dff["CUSTOMER_ID"].nunique()
col1.metric("Clientes que consumen video", f"{num_clients}")

# KPI 2: Género más visto
top_genre = dff.groupby("GENRE")["SCREENTIME"].sum().idxmax()
col2.metric("Género más visto", f"{top_genre}")

# KPI 3: Usuarios multi-dispositivo
device_count = dff.groupby("CUSTOMER_ID")["DEVICE"].nunique()
multi_device_pct = (device_count > 1).mean() * 100
col3.metric("Usuarios multi-dispositivo", f"{multi_device_pct:.1f}%")

# ----------------- Gráficos -----------------
st.markdown("---")
st.subheader("Tiempo de pantalla por género")
fig_genre = px.bar(dff.groupby("GENRE", as_index=False)["SCREENTIME"].sum(),
                   x="GENRE", y="SCREENTIME", title="Tiempo de pantalla por género")
st.plotly_chart(fig_genre, use_container_width=True)

st.subheader("Distribución de dispositivos")
fig_device = px.pie(dff, names="DEVICE", title="Distribución de dispositivos")
st.plotly_chart(fig_device, use_container_width=True)

st.subheader("Evolución del consumo en el tiempo")
dff_time = dff.groupby("DATE", as_index=False)["SCREENTIME"].sum()
fig_time = px.line(dff_time, x="DATE", y="SCREENTIME", title="Evolución del consumo")
st.plotly_chart(fig_time, use_container_width=True)

st.subheader("Consumo por región")
fig_region = px.bar(dff.groupby("REGION", as_index=False)["SCREENTIME"].sum(),
                    x="REGION", y="SCREENTIME", title="Consumo por región")
st.plotly_chart(fig_region, use_container_width=True)

st.subheader("Top 10 contenido más visto")
top_content = dff.groupby("TITLE")["SCREENTIME"].sum().sort_values(ascending=False).head(10).reset_index()
fig_top = px.bar(top_content, x="TITLE", y="SCREENTIME", title="Top 10 contenido más visto")
st.plotly_chart(fig_top, use_container_width=True)

st.subheader("Recurrencia de consumo por cliente")
consumption_counts = dff.groupby("CUSTOMER_ID").size().reset_index(name="count")
fig_recurrence = px.histogram(consumption_counts, x="count", nbins=20, title="Recurrencia de consumo por cliente")
st.plotly_chart(fig_recurrence, use_container_width=True)

st.subheader("Relación entre región y género")
region_genre = dff.groupby(["REGION", "GENRE"])["SCREENTIME"].sum().reset_index()
fig_heatmap = px.density_heatmap(region_genre, x="REGION", y="GENRE", z="SCREENTIME",
                                 title="Relación entre región y género")
st.plotly_chart(fig_heatmap, use_container_width=True)
#Commit de prueba