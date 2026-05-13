import polars as pl
import os
from dotenv import load_dotenv
load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")

CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
VOLUME_BRONZE = os.environ.get("VOLUME_BRONZE")
ENRICHED_METADATA_CSV = os.environ.get("ENRICHED_METADATA_CSV")
ENRICHED_DOI_JCR = os.environ.get("ENRICHED_DOI_JCR")
OUTPUT_ARTIGOS_BR = os.environ.get("OUTPUT_ARTIGOS_BR")


# Caminhos
METADATA_VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/metadata.csv"
ENRICHED_DOI_JCR_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/{ENRICHED_DOI_JCR}"

PATH_BRASILEIROS = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/{OUTPUT_ARTIGOS_BR}"


METADATA_ENRICHED_VOLUME_PATH = f"Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_BRONZE}/{ENRICHED_METADATA_CSV}"

def generate_enriched_metadata():
    if not os.path.exists(ENRICHED_DOI_JCR_PATH):
        print(f"Erro: Mapeamento JCR não encontrado.")
        return
    
    if not os.path.exists(PATH_BRASILEIROS):
        print(f"Erro: Arquivo de papers brasileiros não encontrado.")
        return

    print("Lendo metadata original...")
    df_metadata = pl.read_csv(METADATA_VOLUME_PATH, infer_schema_length=0)

    if "doi" in df_metadata.columns:
        df_metadata = df_metadata.rename({"doi": "DOI"})

    print("Lendo mapeamento JCR...")
    df_jcr = pl.read_parquet(ENRICHED_DOI_JCR_PATH)

    print("Lendo papers brasileiros...")
    # Selecionamos apenas paper_id para o join e criamos o flag
    df_br = pl.read_csv(PATH_BRASILEIROS).select([
        pl.col("paper_id").alias("sha"),
        pl.lit(1).alias("paper_br")
    ]).unique(subset=["sha"])

    # Limpeza de strings para garantir o match em DOI e SHA
    df_metadata = df_metadata.with_columns([
        pl.col("DOI").str.strip_chars(),
        pl.col("sha").str.strip_chars()
    ])
    df_jcr = df_jcr.with_columns(pl.col("DOI").str.strip_chars())
    df_br = df_br.with_columns(pl.col("sha").str.strip_chars())

    print("Realizando Joins (JCR e Brasileiros)...")
    df_final = (
        df_metadata
        .join(df_jcr, on="DOI", how="left")
        .join(df_br, on="sha", how="left")
        .with_columns(
            pl.col("paper_br").fill_null(0) # Transforma nulos em 0 para facilitar filtros
        )
    )

    # Auditoria
    total = df_final.shape[0]
    filled_jcr = df_final.filter(pl.col("Rank").is_not_null()).shape[0]
    filled_br = df_final.filter(pl.col("paper_br") == 1).shape[0]
    
    print(f"\nResultado:")
    print(f"- Total de linhas: {total}")
    print(f"- Linhas com JCR: {filled_jcr} ({(filled_jcr/total)*100:.2f}%)")
    print(f"- Papers Brasileiros identificados: {filled_br}")

    print(f"Salvando em: {METADATA_ENRICHED_VOLUME_PATH}")
    df_final.write_csv(METADATA_ENRICHED_VOLUME_PATH)

if __name__ == "__main__":
    generate_enriched_metadata()