import os
from dotenv import load_dotenv
import pandas as pd
from sentence_transformers import SentenceTransformer
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import ArrayType, StringType, FloatType


load_dotenv()

# Nome da tabela final na Unity Catalog
GOLD_VOLUME_PARQUET_PATH = os.environ.get("GOLD_VOLUME_PARQUET_PATH")
TARGET_TABLE_RAG_NAME = os.environ.get("TARGET_TABLE_RAG_NAME")
EMBEDDING_MODEL_NAME = os.environ.get("EMBEDDING_MODEL_NAME")

@pandas_udf(ArrayType(FloatType()))
def get_embeddings(texts: pd.Series) -> pd.Series:

    # Configure writable cache directories for HuggingFace/Transformers on workers
    os.environ['TRANSFORMERS_CACHE'] = '/tmp/transformers_cache'
    os.environ['HF_HOME'] = '/tmp/hf_home'
    os.environ['SENTENCE_TRANSFORMERS_HOME'] = '/tmp/sentence_transformers'

    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    embeddings = model.encode(texts.tolist(), show_progress_bar=False)
    return pd.Series(embeddings.tolist())

if __name__ == "__main__":
    # 1. Carregar dados do Parquet (Potencialmente novos)
    df_new_gold = spark.read.parquet(GOLD_VOLUME_PARQUET_PATH)

    # 2. Verificar o que já existe na UC para evitar re-trabalho
    if spark.catalog.tableExists(TARGET_TABLE_RAG_NAME):
        df_existing = spark.table(TARGET_TABLE_RAG_NAME).select("sha_id", "chunk_text").distinct()
        
        # Identificar apenas o que é NOVO (Left Anti Join)
        # Comparamos SHA e o texto do chunk para garantir que mudanças no texto disparem novo embedding
        df_to_process = df_new_gold.join(df_existing, ["sha_id", "chunk_text"], "left_anti")
    else:
        df_to_process = df_new_gold

    # 3. Se houver novos dados, calcular embeddings
    if df_to_process.count() > 0:
        print(f"Processando {df_to_process.count()} novos chunks...")
        
        # Repartition to smaller chunks to avoid OOM errors
        df_to_process = df_to_process.repartition(20)
        

        df_final = df_to_process.withColumn("embedding", get_embeddings(col("chunk_text")))

        # 4. Salvar/Append na Tabela UC com CDF habilitado
        (df_final.write
        .format("delta")
        .mode("append") # Sempre append para manter o histórico
        .option("mergeSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.columnMapping.mode", "name")
        .saveAsTable(TARGET_TABLE_RAG_NAME))
        
        print("Novos embeddings integrados à tabela UC.")
    else:
        print("Nenhum dado novo detectado. Tabela já está sincronizada.")
