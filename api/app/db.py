from google.cloud import storage
import duckdb
from pathlib import Path

BUCKET_NAME = "dw-bucket-storage"
BLOB_NAME = "gold/app_terceirizados/app_terceirizados.parquet"

LOCAL_PARQUET_PATH = Path("/tmp/app_terceirizados.parquet")
LOCAL_DB_PATH = Path("/tmp/app.duckdb")


def download_parquet():
    if LOCAL_PARQUET_PATH.exists():
        return

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BLOB_NAME)

    blob.download_to_filename(LOCAL_PARQUET_PATH)


def initialize_duckdb():
    """
    Cria banco local e registra tabela ouro.app_terceirizados
    """
    if LOCAL_DB_PATH.exists():
        return

    download_parquet()

    con = duckdb.connect(str(LOCAL_DB_PATH))

    # Cria schema
    con.execute("CREATE SCHEMA IF NOT EXISTS ouro;")

    # Cria tabela a partir do parquet
    con.execute(
        f"""
        CREATE TABLE ouro.app_terceirizados AS
        SELECT * FROM read_parquet('{LOCAL_PARQUET_PATH}');
    """
    )

    con.close()


def get_connection():
    initialize_duckdb()
    return duckdb.connect(str(LOCAL_DB_PATH))
