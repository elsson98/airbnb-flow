import time

from prefect import flow, variables, task
from prefect_dbt.cli.commands import DbtCoreOperation


@task(name="Create seeds")
def create_seeds() -> str:
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt seed"],
        project_dir=variables.get('project_dir'),
        profiles_dir=variables.get('dbt_dir'),
    ).run()
    return result


@task(name="Run Models")
def run_models() -> str:
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt run"],
        project_dir=variables.get('project_dir'),
        profiles_dir=variables.get('dbt_dir'),
    ).run()
    return result


@task(name="Run individual and general tests")
def run_test() -> str:
    result = DbtCoreOperation(
        commands=["pwd", "dbt debug", "dbt test"],
        project_dir=variables.get('project_dir'),
        profiles_dir=variables.get('dbt_dir'),
    ).run()
    return "success"


@flow(name="Dbt transformation pipeline")
def dbt_transformation():
    create_seeds()
    run_models()
    run_test()
