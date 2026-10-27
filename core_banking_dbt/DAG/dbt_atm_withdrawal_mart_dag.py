from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/projects/core_banking_dbt"
PROFILES_DIR = "/opt/airflow/.dbt"

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="atm_withdraw_txn_data_mart_refresh",
    description="Incrementally refresh and test ATM withdrawal mart every 30 minutes",
    start_date=datetime(2025, 8, 2, 0, 0),
    schedule_interval="*/10 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["dbt", "atm_withdrawal", "gold"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_atm_withdraw_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select +atm_withdraw_txn_data_mart --profiles-dir {PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test_atm_withdraw_mart",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select atm_withdraw_txn_data_mart --profiles-dir {PROFILES_DIR}"
        ),
    )

    dbt_run >> dbt_test






