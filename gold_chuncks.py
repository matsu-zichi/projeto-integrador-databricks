import os
import pandas as pd
from dotenv import load_dotenv
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col, concat_ws, explode, split, trim, substring, lit

load_dotenv()

# Caminhos
SILVER_JSON_PATH = os.environ.get("SILVER_JSON_PATH")
GOLD_VOLUME_PARQUET_PATH = os.environ.get("GOLD_VOLUME_PARQUET_PATH")
ENRICHED_METADATA_PATH = os.environ.get("ENRICHED_METADATA_PATH")


def chunk_text(text):
    if not text: return []
    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=150)
    return splitter.split_text(text)

if __name__ == "__main__":
    print("Lendo os JSONs brutos")
    raw_jsons = spark.read.option("recursiveFileLookup", "true").json(SILVER_JSON_PATH)
    print("Lendo os Metadados Enriquecidos")
    df_final_selection = (
                spark
                .read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(ENRICHED_METADATA_PATH)
            )
    
    # 2. Extraindo texto e IDs
    # Nota: O paper_id do JSON deve bater com o sha_individual do seu metadado
    print("Extraindo IDs e Textos")
    df_texts = raw_jsons.select(
        col("paper_id").alias("sha_id"),
        concat_ws(" ", col("abstract.text")).alias("abstract"),
        concat_ws(" ", col("body_text.text")).alias("body")
    ).withColumn("full_content", concat_ws("\n\n", col("abstract"), col("body")))

    # 3. Join com o Metadado Enriquecido (usando o DataFrame que criamos no passo anterior)
    print("JOIN do conteúdo com os metadados")
    df_enriched = df_texts.join(
        df_final_selection.select("sha", "title", "authors", "publish_time", "Full Journal Title", "Rank", "paper_br"),
        df_texts.sha_id == df_final_selection.sha,
        "inner"
    ).drop("sha")

    # 4. Chunking do conteúdo
    print("Chunking do conteúdo")
    chunk_udf = udf(chunk_text, ArrayType(StringType()))

    print("Gerando Chunks e salvando em Parquet no Volume Gold")
    df_gold_intermediate = (df_enriched
                            .withColumn("chunk_text", explode(chunk_udf(col("full_content"))))
                            .select("sha_id", "title", "authors", "publish_time", "Full Journal Title", "Rank", "paper_br", "chunk_text"))

    df_gold_intermediate.write.mode("overwrite").parquet(GOLD_VOLUME_PARQUET_PATH)
    print("Camada Gold Parquet atualizada com sucesso.")