import requests
import pandas as pd
import os
from urllib.parse import urlparse, parse_qs

def download_and_enrich_data():
    api_url = "https://api.github.com/repos/digitalinnovationone/netflix-dataset/contents/raw"
    base_raw_url = "https://raw.githubusercontent.com/digitalinnovationone/netflix-dataset/main/raw/"
    
    # Configuração de caminhos
    current_dir = os.path.dirname(__file__)
    bronze_path = os.path.abspath(os.path.join(current_dir, "../../../data/bronze"))
    os.makedirs(bronze_path, exist_ok=True)
    
    paises_map = {"brasil": "br", "france": "fr", "italian": "it"}

    print("🚀 Iniciando Ingestão com df_temp (Camada Bronze)...")
    response = requests.get(api_url)
    
    if response.status_code == 200:
        repo_files = response.json()
        
        for file in repo_files:
            file_name_raw = file['name']
            file_name_lower = file_name_raw.lower()
            sigla = next((sgl for pais, sgl in paises_map.items() if pais in file_name_lower), None)
            
            if sigla:
                full_url = f"{base_raw_url}{file_name_raw}"
                try:
                    df_temp = pd.read_excel(full_url)
                    df_temp['localizacao'] = sigla
                    
                    def extract_utms(url):
                        if pd.isna(url) or '?' not in str(url): return {}
                        params = parse_qs(urlparse(url).query)
                        return {k: v[0] for k, v in params.items()}

                    utm_cols = df_temp['utm_link'].apply(extract_utms).apply(pd.Series)
                    df_temp = pd.concat([df_temp, utm_cols], axis=1)
                    
                    parts = file_name_raw.replace(".xlsx", "").split('_')
                    parquet_name = f"{parts[0]}_{parts[1]}_{sigla}.parquet"
                    full_output_path = os.path.join(bronze_path, parquet_name)
                    
                    df_temp.to_parquet(full_output_path, index=False, engine='pyarrow')
                    print(f"✅ Arquivo processado: {parquet_name}")

                except Exception as e:
                    print(f"❌ Erro ao processar {file_name_raw}: {e}")

        print("\n🔥 Camada Bronze finalizada com sucesso!")
        
        
        # Cria o arquivo de sucesso DENTRO da pasta data/bronze
        success_file = os.path.join(bronze_path, "_SUCCESS")
        with open(success_file, "w") as f:
            f.write("done")
            
    else:
        print(f"🔴 Erro na API: {response.status_code}")

if __name__ == "__main__":
    download_and_enrich_data()