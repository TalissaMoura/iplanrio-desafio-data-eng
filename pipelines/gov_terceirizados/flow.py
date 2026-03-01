from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from pathlib import Path
from datetime import datetime
import duckdb
import os
import dotenv

# PIPELINE CONFIG
REF_DATE = datetime.now().strftime("%Y-%m")

DBT_PROJECT_DIR = Path("dw")
DBT_PROFILES_DIR = DBT_PROJECT_DIR / ".dbt"

## PROJECT DIRECTORIES
BRONZE_DIR = Path("/app/dw/models/staging")
SILVER_DIR = Path("/app/dw/models/core")
DIMENSIONS_DIR = SILVER_DIR / "dimensions"
FACTS_DIR = SILVER_DIR / "facts"
GOLD_DIR = Path("/app/dw/models/mart")

# PROJECT BUCKETS
RAW_BUCKET = "gs://dw-bucket-storage/raw"
BRONZE_BUCKET = "gs://dw-bucket-storage/bronze"
SILVER_BUCKET = "gs://dw-bucket-storage/silver"
GOLD_BUCKET = "gs://dw-bucket-storage/gold"

# Load environment variables from .env file


def export_to_gcs(model_name: str, schema: str, path_to_parquet: str):
    logger = get_run_logger()
    dotenv.load_dotenv("/app/.env")
    con = duckdb.connect("/app/dw/dev.duckdb")
    # set credenciais do GCS para o DuckDB
    # 1. Instalar e carregar extensões necessárias para nuvem
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # 2. Configurar as credenciais GCS na sessão atual
    con.execute(f"SET s3_access_key_id='{os.environ.get('GCS_ACCESS_ID')}';")
    con.execute(f"SET s3_secret_access_key='{os.environ.get('GCS_SECRET')}';")
    con.execute("SET s3_endpoint='storage.googleapis.com';")

    logger.info(f"Exportando {model_name} para {path_to_parquet}...")
    try:
        con.execute(
            f"COPY main_{schema}.{model_name} TO '{path_to_parquet}' (FORMAT PARQUET)"
        )
    except Exception as e:
        logger.error(f"Erro ao exportar {model_name} para GCS: {e}")
    con.close()


def run_dbt_commands(commands: list[list[str]], vars: dict | None = None) -> None:
    logger = get_run_logger()

    if dotenv.load_dotenv("/app/.env"):
        logger.info("Variáveis de ambiente carregadas com sucesso!")
        logger.info(f"GCS_ACCESS_ID: {os.environ.get('GCS_ACCESS_ID')}")
        logger.info(f"GCS_SECRET: {os.environ.get('GCS_SECRET')}")
    else:
        raise ValueError(
            "Não foi possível carregar as variáveis de ambiente do arquivo .env"
        )

    settings = PrefectDbtSettings(
        project_dir=str(DBT_PROJECT_DIR), profiles_dir=str(DBT_PROFILES_DIR)
    )

    runner = PrefectDbtRunner(
        settings=settings,
        raise_on_failure=True,
    )

    for cmd_parts in commands:
        if vars:
            import json

            vars_string = json.dumps(vars)
            cmd_parts = cmd_parts + ["--vars", vars_string]

        logger.info(f"Executando: dbt {' '.join(cmd_parts)}")
        result = runner.invoke(cmd_parts)

        if not result.success:
            raise Exception(f"Erro ao executar dbt {' '.join(cmd_parts)}")


@task(name="Run Bronze Layer")
def dbt_run_bronze(partition: str = "*"):
    if partition == "*":
        parquet_path = "*.parquet"
    else:
        parquet_path = f"terceirizados_{partition}.parquet"

    run_dbt_commands(
        commands=[["run", "--select", "brutos_terceirizados"]],
        vars={"parquet_path": RAW_BUCKET + f"/{parquet_path}"},
    )

    export_to_gcs(
        model_name="brutos_terceirizados",
        schema="bronze",
        path_to_parquet=str(BRONZE_BUCKET + "/brutos_tercerizados.parquet"),
    )


@task(name="Run Silver Dimensions")
def dbt_run_silver_dims(dimensions_dir: Path):
    sql_files = sorted([file.name for file in dimensions_dir.rglob("*.sql")])

    for sql_file in sql_files:
        model_name, _ = sql_file.split(".")  # nome do arquivo sem .sql

        run_dbt_commands(commands=[["run", "--select", model_name]])

        export_to_gcs(
            model_name=model_name,
            schema="prata",
            path_to_parquet=str(
                SILVER_BUCKET + f"/{model_name}" + f"/{model_name}.parquet"
            ),
        )


@task(name="Run Silver Facts")
def dbt_run_silver_facts(facts_dir: Path):
    sql_files = sorted([file.name for file in facts_dir.rglob("*.sql")])

    for sql_file in sql_files:
        model_name, _ = sql_file.split(".")  # nome do arquivo sem .sql

        run_dbt_commands(commands=[["run", "--select", model_name]])

        export_to_gcs(
            model_name=model_name,
            schema="prata",
            path_to_parquet=str(
                SILVER_BUCKET + f"/{model_name}" + f"/{model_name}.parquet"
            ),
        )


@task(name="Run Gold Layer")
def dbt_run_gold():
    run_dbt_commands(commands=[["run", "--select", "mart_terceirizados"]])

    export_to_gcs(
        model_name="mart_terceirizados",
        schema="gold",
        path_to_parquet=str(GOLD_BUCKET + "/mart_terceirizados.parquet"),
    )


@flow(name="terceirizados-pipeline")
def gov_terceirizados_flow(
    partition: str = REF_DATE,
    dimensions_dir: Path = DIMENSIONS_DIR,
    facts_dir: Path = FACTS_DIR,
):
    """
    Pipeline completo:
    raw (parquet) -> bronze (merge) -> silver -> gold
    """

    dbt_run_bronze(partition=partition)
    dbt_run_silver_dims(dimensions_dir=dimensions_dir)
    dbt_run_silver_facts(facts_dir=facts_dir)
    dbt_run_gold()


if __name__ == "__main__":
    gov_terceirizados_flow(
        dimensions_dir=DIMENSIONS_DIR, facts_dir=FACTS_DIR
    )
