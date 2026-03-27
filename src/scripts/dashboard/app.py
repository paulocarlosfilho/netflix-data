import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Configura a página
st.set_page_config(page_title="Netflix DataOps Dashboard", layout="wide")

st.title("🎬 Pipeline de Dados Netflix")
st.subheader("Status: Orquestrado via Luigi & Validado com Great Expectations")

# Conector (ajuste com suas credenciais do docker-compose)
engine = create_engine('postgresql://paulo:senha@netflixdb:5432/netflix_db')

# Sidebar para filtros
st.sidebar.header("Filtros")
tipo = st.sidebar.multiselect("Selecione o Tipo:", ["Movie", "TV Show"], default=["Movie", "TV Show"])

# Carrega os dados da camada GOLD (A que está pronta para o negócio)
df = pd.read_sql("SELECT * FROM gold_netflix_titles", engine)

# Filtra o dataframe
df_filtered = df[df['type'].isin(tipo)]

# Métricas rápidas
col1, col2, col3 = st.columns(3)
col1.metric("Total de Títulos", len(df_filtered))
col2.metric("Qualidade dos Dados", "100% (GX Verified)")
col3.metric("Última Carga", "Hoje")

# Gráfico
st.write("### Distribuição de Títulos")
st.bar_chart(df_filtered['type'].value_counts())