from prefect import flow, task, get_run_logger
from prefect_dbt.cli import DbtCoreOperation


DBT_PROJECT_DIR = "/app/dbt"  # ajuste se necessário
DBT_PROFILES_DIR = "/app/dbt"  # ajuste se necessário


@task
def dbt_deps():
    logger = get_run_logger()
    logger.info("Instalando dependências do dbt...")

    result = DbtCoreOperation(
        commands=["dbt deps"],
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    ).run()

    return result


@task
def dbt_run_ouro():
    logger = get_run_logger()
    logger.info("Executando modelo ouro.gov_tercerizados...")

    result = DbtCoreOperation(
        commands=["dbt run --select ouro.gov_tercerizados"],
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    ).run()

    return result


@flow(name="gov-tercerizados-dbt-flow")
def gov_tercerizados_flow():
    """
    Flow que executa o modelo ouro.gov_tercerizados via dbt
    """
    dbt_deps()
    dbt_run_ouro()


if __name__ == "__main__":
    gov_tercerizados_flow()
