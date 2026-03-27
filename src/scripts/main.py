import luigi
import os
import sys
import time

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
src_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if src_root not in sys.path:
    sys.path.append(src_root)

from scripts.extracao_parquet.extracao_bronze import download_and_enrich_data
from scripts.transform_parquet.transform_silver import processar_silver_para_ml
from scripts.load_parquet.load_parquet import salvar_camada_gold

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
        with self.output().open('w') as f:
            f.write('sucesso')

class EtapaGold(luigi.Task):
    def requires(self):
        return EtapaSilver()

    def output(self):
        return luigi.LocalTarget(os.path.join(project_root, "data/gold/dataset_final_gold.parquet"))

    def run(self):
        print("\n🥇 [LUIGI] Iniciando Camada Gold (ML)...")
        salvar_camada_gold()

if __name__ == "__main__":
    luigi.run(["EtapaGold", "--local-scheduler"])
