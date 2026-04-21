import luigi
import os
import sys
import pandas as pd
import great_expectations as gx
import json
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult

# Configuração de caminhos
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
src_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if src_root not in sys.path:
    sys.path.append(src_root)

from scripts.extracao_parquet.extracao_bronze import download_and_enrich_data
from scripts.transform_parquet.transform_silver import processar_silver_para_ml
from scripts.load_parquet.load_parquet import salvar_camada_gold

# Inicializa o contexto
context = gx.get_context(project_root_dir=project_root)

class EtapaBronze(luigi.Task):
    def output(self):
        return luigi.LocalTarget(os.path.join(project_root, "data/bronze/_SUCCESS"))

    def run(self):
        print("\n🥉 [LUIGI] Iniciando Camada Bronze...")
        download_and_enrich_data()
        os.makedirs(os.path.join(project_root, "data/bronze"), exist_ok=True)
        with self.output().open('w') as f:
            f.write('sucesso')

class EtapaSilver(luigi.Task):
    def requires(self):
        return EtapaBronze()

    def output(self):
        return luigi.LocalTarget(os.path.join(project_root, "data/silver/_SUCCESS_SILVER"))

    def run(self):
        print("\n🥈 [LUIGI] Iniciando Camada Silver...")
        processar_silver_para_ml()
        os.makedirs(os.path.join(project_root, "data/silver"), exist_ok=True)
        with self.output().open('w') as f:
            f.write('sucesso')

class EtapaValidacaoSilver(luigi.Task):
    def requires(self):
        return EtapaSilver()

    def output(self):
        return luigi.LocalTarget(os.path.join(project_root, "data/silver/_SUCCESS_VALIDATION"))

    def run(self):
        print("\n✅ [LUIGI] Iniciando Validação da Camada Silver...")
        
        silver_path = os.path.join(project_root, "data/silver/tabela_silver_pronta_para_ml.parquet")
        suite_path = os.path.join(project_root, "great_expectations/expectations/silver_weather_suite.json")
        
        if not os.path.exists(silver_path):
            raise Exception(f"Arquivo de dados não encontrado: {silver_path}")
        if not os.path.exists(suite_path):
            raise Exception(f"Suite não encontrada: {suite_path}")
            
        df_silver = pd.read_parquet(silver_path)

        # Carrega o nome da suite do JSON
        with open(suite_path, 'r') as f:
            suite_dict = json.load(f)
        suite_name = suite_dict.get("expectation_suite_name", "silver_weather_suite")

        try:
            datasource_name = "temporary_pandas_datasource"
            if datasource_name in context.datasources:
                context.delete_datasource(datasource_name)
            
            datasource = context.sources.add_pandas(name=datasource_name)
            data_asset = datasource.add_dataframe_asset(name="silver_asset")
            
            # Cria o batch request e o validator
            batch_request = data_asset.build_batch_request(dataframe=df_silver)
            
            validator = context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name,
            )

            print(f"📊 Validando {len(df_silver)} registros...")
            results = validator.validate()

            if not results["success"]:
                print("❌ [LUIGI] Validação reprovada!")
                context.build_data_docs()
                raise Exception("Dados fora do padrão. Verifique os Data Docs.")

            print("✅ [LUIGI] Validação concluída com sucesso!")
            context.build_data_docs()

        except Exception as e:
            print(f"❌ Erro no Great Expectations: {e}")
            raise e

        with self.output().open('w') as f:
            f.write('sucesso')

class EtapaGold(luigi.Task):
    def requires(self):
        return EtapaValidacaoSilver()

    def output(self):
        return luigi.LocalTarget(os.path.join(project_root, "data/gold/dataset_final_gold.parquet"))

    def run(self):
        print("\n🥇 [LUIGI] Iniciando Camada Gold...")
        os.makedirs(os.path.join(project_root, "data/gold"), exist_ok=True)
        salvar_camada_gold()
        print("\n🔥 [SUCESSO] Pipeline Finalizado!")

if __name__ == "__main__":
    luigi.run()