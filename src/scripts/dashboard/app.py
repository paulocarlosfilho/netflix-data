import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# Configurações de conexão
def get_engine():
    user = os.getenv('DB_USER', 'postgres')
    pwd = os.getenv('DB_PASS', 'postgres')
    host = os.getenv('DB_HOST', 'db')
    port = os.getenv('DB_PORT', '5432')
    db = os.getenv('DB_NAME', 'netflixdb')
    return create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

st.set_page_config(page_title="Netflix Business Intelligence", layout="wide")

# Estilo customizado para os cards
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #3e4253; }
    </style>
    """, unsafe_allow_html=True)

st.title("🎬 Netflix Data & ML Dashboard")
st.markdown("Pipeline consolidado: **Bronze ➔ Silver ➔ Gold**")

@st.cache_data(ttl=300)
def load_data():
    engine = get_engine()
    # Puxamos todos os dados da Gold que seu script gerou
    return pd.read_sql("SELECT * FROM gold_netflix_data", engine)

try:
    df = load_data()

    # --- BARRA LATERAL (FILTROS) ---
    st.sidebar.header("Filtros")
    paises = st.sidebar.multiselect("Filtrar por Localização", options=df['localizacao'].unique(), default=df['localizacao'].unique())
    
    df_filtered = df[df['localizacao'].isin(paises)]

    # --- KPIs PRINCIPAIS ---
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Registros Processados", len(df_filtered))
    m2.metric("Faturamento Real (Total)", f"R$ {df_filtered['Amount'].sum():,.2f}")
    m3.metric("Previsão ML (Total)", f"R$ {df_filtered['previsao_amount'].sum():,.2f}")
    m4.metric("Idade Média", f"{df_filtered['Age'].mean():.1f} anos")

    st.markdown("---")

    # --- GRÁFICOS ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📍 Real vs Previsto por Localização")
        # Comparando o que aconteceu de verdade com o que o seu RandomForest previu
        df_comp = df_filtered.groupby('localizacao')[['Amount', 'previsao_amount']].sum().reset_index()
        fig_city = px.bar(df_comp, x='localizacao', y=['Amount', 'previsao_amount'], 
                          barmode='group', color_discrete_sequence=['#E50914', '#564d4d'],
                          labels={'value': 'Valor', 'variable': 'Tipo'})
        st.plotly_chart(fig_city, use_container_width=True)

    with col2:
        st.subheader("📊 Distribuição por Faixa Etária")
        fig_age = px.histogram(df_filtered, x="Age", nbins=20, 
                               color_discrete_sequence=['#E50914'],
                               labels={'Age': 'Idade', 'count': 'Frequência'})
        st.plotly_chart(fig_age, use_container_width=True)

    # --- TABELA DETALHADA ---
    with st.expander("🔍 Explorar Dados Brutos (Camada Gold)"):
        st.write(df_filtered)

except Exception as e:
    st.error(f"Erro ao carregar dashboard: {e}")
    st.info("Verifique se o banco de dados está online e se a tabela 'gold_netflix_data' existe.")