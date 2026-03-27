import pandas as pd
import glob
import os

def processar_silver_para_ml():
    # Caminhos
    bronze_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/bronze"))
    silver_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../data/silver"))
    os.makedirs(silver_path, exist_ok=True)

    # Busca todos os parquets que você gerou
    arquivos = glob.glob(os.path.join(bronze_path, "*.parquet"))
    
    if not arquivos:
        print("Pô, não achei nenhum arquivo na Bronze! Roda o script de ingestão primeiro.")
        return

    df_silver = pd.concat([pd.read_parquet(f) for f in arquivos], ignore_index=True)
    
    # O modelo de ML não lê "summer", ele lê "1".
    if 'utm_campaign' in df_silver.columns:
        df_silver['campanha_cod'] = df_silver['utm_campaign'].astype('category').cat.codes

    
    df_silver['localizacao_cod'] = df_silver['localizacao'].astype('category').cat.codes

    
    df_silver['Amount'] = df_silver['Amount'].fillna(0)

    # Salva a Tabela Master
    output_file = os.path.join(silver_path, "tabela_silver_pronta_para_ml.parquet")
    df_silver.to_parquet(output_file, index=False)
    
    print(f"✅ Camada Silver Gerada! {len(df_silver)} linhas prontas para o modelo de ML.")
    return df_silver

if __name__ == "__main__":
    df_final = processar_silver_para_ml()
    print(df_final.head())
