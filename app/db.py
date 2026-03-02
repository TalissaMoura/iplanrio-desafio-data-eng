import duckdb
from pathlib import Path

DB_PATH = Path("data/ouro/app.duckdb")
PARQUET_PATH = Path("data/ouro/app_terceirizados.parquet")


def get_connection():
    conn = duckdb.connect(DB_PATH)

    # cria view se não existir
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW app_terceirizados AS
        SELECT *
        FROM read_parquet('{PARQUET_PATH}')
    """
    )

    return conn
