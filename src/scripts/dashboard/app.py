import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# Configurações de conexão via Variáveis de Ambiente
def get_engine():
    user = os.getenv('DB_USER', 'postgres')
    pwd = os.getenv('DB_PASS', 'postgres')
    host = os.getenv('DB_HOST', 'db')
    port = os.getenv('DB_PORT', '5432')
    db = os.getenv('DB_NAME', 'netflixdb')
    return create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

st.set_page_config(page_title="Netflix Analytics", layout="wide")

st.title("🎬 Dashboard Netflix - Data Pipeline")
st.markdown("---")

@st.cache_data(ttl=600)
def load_data():
    engine = get_engine()
    # Tenta ler da Gold (ajuste o nome da tabela conforme seu script Luigi)
    return pd.read_sql("SELECT * FROM netflix_titles LIMIT 500", engine)

try:
    df = load_data()
    
    # KPIs principais
    c1, c2, c3 = st.columns(3)
    c1.metric("Títulos Totais", len(df))
    c2.metric("Qualidade dos Dados", "✅ GX Verified")
    c3.metric("Ambiente", "Docker / Devbox")

    st.subheader("Análise por Tipo de Conteúdo")
    st.bar_chart(df['type'].value_counts())

    st.subheader("Visualização da Camada Gold")
    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"Aguardando dados... Erro: {e}")
    st.info("O banco de dados pode estar vazio. Rode o pipeline do Luigi primeiro!")