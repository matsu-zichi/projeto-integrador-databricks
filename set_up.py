import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

load_dotenv()

w = WorkspaceClient()

CATALOG_NAME = os.environ.get("CATALOG_NAME")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME")
VOLUMES = [os.environ.get("VOLUME_BRONZE"), os.environ.get("VOLUME_SILVER"), os.environ.get("VOLUME_GOLD")]

def execute_sql(sql_query):
    """
    Executa comandos SQL via SDK.
    Nota: Requer um SQL Warehouse ativo.
    """
    # Na Community Edition, tentamos pegar o primeiro Warehouse disponível
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise Exception("Nenhum SQL Warehouse encontrado. Crie um na aba 'SQL Warehouse' ou use um Notebook.")

    warehouse_id = warehouses[0].id

    print(f"Executando SQL: {sql_query[:50]}...")
    res = w.statement_execution.execute_statement(
        statement=sql_query,
        warehouse_id=warehouse_id
    )
    return res

def main():
    w = WorkspaceClient()

    # --- 1. PROCESSO PARA O CATÁLOGO ---
    try:
        print(f"\nTentando acessar catálogo: {CATALOG_NAME}...")
        w.catalogs.get(CATALOG_NAME)
        print(f"Catálogo '{CATALOG_NAME}' já existe.")
    except NotFound:
        print(f"Catálogo não encontrado. Criando via SQL...")
        execute_sql(f"CREATE CATALOG {CATALOG_NAME};")
        print(f"Catálogo '{CATALOG_NAME}' criado.")
    except Exception as e:
        print(f"Erro inesperado no catálogo: {e}")

    # --- 2. PROCESSO PARA O SCHEMA ---
    try:
        # O get de schema exige o nome completo: catalogo.schema
        full_schema_name = f"{CATALOG_NAME}.{SCHEMA_NAME}"
        print(f"\nTentando acessar schema: {full_schema_name}...")
        w.schemas.get(full_schema_name)
        print(f"Schema '{SCHEMA_NAME}' já existe.")
    except NotFound:
        print(f"Schema não encontrado. Criando via SQL...")
        execute_sql(f"CREATE SCHEMA {CATALOG_NAME}.{SCHEMA_NAME};")
        print(f"Schema '{SCHEMA_NAME}' criado.")
    except Exception as e:
        # Caso o catálogo tenha acabado de ser criado, o schema pode dar erro de 'Parent not found'
        print(f"Erro ao acessar/criar schema: {e}")

    # --- 3. PROCESSO PARA O VOLUME ---
        # O get de volume exige o nome completo: catalogo.schema.volume
    for vol in VOLUMES:
        try:
            full_volume_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{vol}"
            print(f"\nTentando acessar volume: {full_volume_name}...")
            w.volumes.read(full_volume_name)
            print(f"Volume '{vol}' já existe.")
        except NotFound:
            print(f"Volume não encontrado. Criando via SQL...")
            execute_sql(f"CREATE VOLUME {CATALOG_NAME}.{SCHEMA_NAME}.{vol};")
            print(f"Volume '{vol}' criado.")
        except Exception as e:
            print(f"Erro ao acessar/criar volume: {e}")


if __name__ == "__main__":
    main()