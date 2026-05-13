import os
from dotenv import load_dotenv
from pyspark.sql.functions import col, lower, trim, when

load_dotenv()

CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
BRONZE = os.environ.get('VOLUME_BRONZE')
SILVER = os.environ.get('VOLUME_SILVER')
ENRICHED_METADATA_CSV = os.environ.get('ENRICHED_METADATA_CSV')
ENRICHED_METADATA_TABLE = os.environ.get('ENRICHED_METADATA_TABLE')

ENRICHED_METADATA_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{BRONZE}/{ENRICHED_METADATA_CSV}"
ENRICHED_METADATA_TABLE_PATH = f"{CATALOG_NAME}.{SCHEMA_NAME}.{ENRICHED_METADATA_TABLE}"

# Mapeamento manual para garantir que todos os nomes sigam o padrão snake_case
mapping = {
    "cord_uid": "cord_uid",
    "sha": "sha",
    "source_x": "source_x",
    "title": "title",
    "DOI": "doi",
    "pmcid": "pmcid",
    "pubmed_id": "pubmed_id",
    "license": "license",
    "abstract": "abstract",
    "publish_time": "publish_time",
    "authors": "authors",
    "journal": "journal",
    "mag_id": "mag_id",
    "who_covidence_id": "who_covidence_id",
    "arxiv_id": "arxiv_id",
    "pdf_json_files": "pdf_json_files",
    "pmc_json_files": "pmc_json_files",
    "url": "url",
    "s2_id": "s2_id",
    "ISSN": "issn",
    "ISSN_key": "issn_key",
    "Full Journal Title": "full_journal_title",
    "Rank": "rank",
    "Journal Impact Factor": "journal_impact_factor",
    "paper_br": "paper_br"
}

def limpeza_filtro(df):
    # - paper_br deve ser apenas 0 ou 1
    # - Rank não pode ser NULL e deve ser numérico (o cast para 'double' transforma texto em NULL)
    return df.filter(
        (col("sha").isNotNull()) &
        (col("paper_br").isin("0", "1")) &
        (col("Rank").isNotNull()) &
        (col("publish_time").substr(1, 4) > "2020")
    )


if __name__ == "__main__":
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(ENRICHED_METADATA_PATH)

    df_clean = limpeza_filtro(df)

    # Tentativa de converter Rank para numérico; se houver texto, resultará em NULL e será filtrado
    df_clean = df_clean.withColumn("Rank_numeric", col("Rank").cast("double")) \
        .filter(col("Rank_numeric").isNotNull()) \
        .drop("Rank") \
        .withColumnRenamed("Rank_numeric", "Rank")

    # Aplicar renomeação
    for old_name, new_name in mapping.items():
        if old_name in df_clean.columns:
            df_clean = df_clean.withColumnRenamed(old_name, new_name)

    # Selecionar apenas as colunas mapeadas na ordem desejada
    df_final = df_clean.select(*mapping.values())


    # display(df_final.limit(10))

    # Para salvar como uma tabela no Unity Catalog:
    df_final.write.mode("overwrite").saveAsTable(ENRICHED_METADATA_TABLE)