import pandas as pd
import os
from sklearn.ensemble import RandomForestRegressor
from sqlalchemy import create_engine
from dotenv import load_dotenv

def salvar_camada_gold():
    current_dir = os.path.dirname(__file__)
    silver_path = os.path.abspath(os.path.join(current_dir, "../../../data/silver/tabela_silver_pronta_para_ml.parquet"))
    gold_dir = os.path.abspath(os.path.join(current_dir, "../../../data/gold"))
    os.makedirs(gold_dir, exist_ok=True)

    df_gold = pd.read_parquet(silver_path)

    X = df_gold[['Age', 'campanha_cod', 'localizacao_cod']]
    y = df_gold['Amount']
    
    modelo = RandomForestRegressor(n_estimators=100, random_state=42)
    modelo.fit(X, y)

    df_gold['previsao_amount'] = modelo.predict(X)

    output_path = os.path.join(gold_dir, "dataset_final_gold.parquet")
    df_gold.to_parquet(output_path, index=False)

    print(f"🥇 Camada Gold criada com sucesso!")
    
    print(f"✅ Coluna 'previsao_amount' adicionada ao dataset.")
    
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        try:
            engine = create_engine(db_url, future=True)
            schema = os.getenv("DB_SCHEMA", "public")
            table = os.getenv("DB_TABLE", "dataset_final_gold")
            df_gold.to_sql(table, engine, schema=schema, if_exists="replace", index=False, method="multi", chunksize=1000)
            print(f"✅ Dados salvos no Postgres em {schema}.{table}.")
        except Exception as e:
            print(f"❌ Falha ao salvar no Postgres: {e}")
    else:
        print("ℹ️ DATABASE_URL não definida. Dados salvos apenas em Parquet.")

if __name__ == "__main__":
    salvar_camada_gold()
