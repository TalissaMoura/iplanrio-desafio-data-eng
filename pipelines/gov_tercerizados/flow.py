from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from pathlib import Path


DBT_PROJECT_DIR = "dw"  # ajuste se necessário
DBT_PROFILES_DIR = "dw/.dbt"  # ajuste se necessário



def run_dbt_commands(commands: list[str], project_dir: Path, profiles_dir: Path) -> None:
    """Run dbt commands using the modern prefect-dbt integration.

    Uses PrefectDbtRunner which provides enhanced logging, failure handling,
    and automatic Prefect event emission for dbt node status changes.
    This is much more robust than subprocess calls and integrates natively
    with Prefect's observability features.
    """

    print(f"Running dbt commands: {commands}\n")

    # Configure dbt settings to point to our project directory
    settings = PrefectDbtSettings(
        project_dir=str(project_dir),
        profiles_dir=str(profiles_dir),  
    )

    # Create runner and execute commands
    # Use raise_on_failure=False to handle dbt failures more gracefully
    runner = PrefectDbtRunner(settings=settings, raise_on_failure=False)

    for command in commands:
        clean_command = command.strip() 
        print(f"Executing: dbt {clean_command}")
        # O invoke espera uma lista de argumentos sem o prefixo 'dbt'
        runner.invoke(clean_command.split())

@task(name="Run dbt commands for ouro.brutos_tercerizados")
def dbt_run_ouro():
    run_dbt_commands(
        commands=["run --select brutos_terceirizados"],
        project_dir=Path(DBT_PROJECT_DIR),
        profiles_dir=Path(DBT_PROFILES_DIR)
    )


@flow(name="gov-tercerizados-dbt-flow")
def gov_tercerizados_flow():
    """
    Flow que executa o modelo ouro.gov_tercerizados via dbt
    """
    # dbt_deps()
    dbt_run_ouro()


if __name__ == "__main__":
    gov_tercerizados_flow()
