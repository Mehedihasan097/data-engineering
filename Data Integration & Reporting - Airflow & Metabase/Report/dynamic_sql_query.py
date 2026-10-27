from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def generate_query_and_execute(**kwargs):
    # Initialize the PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_warehouse_dev")

    # Step 1: Fetch distinct warehouse names
    warehouses_query = """
        SELECT DISTINCT warehouse
        FROM warehouse_dev.bin_table;
    """
    warehouses = postgres_hook.get_records(warehouses_query)

    # Step 2: Generate the dynamic SQL
    case_statements = [
        f"SUM(CASE WHEN warehouse = '{warehouse[0]}' THEN actual_qty ELSE 0 END) AS \"{warehouse[0]}\""
        for warehouse in warehouses
    ]
    dynamic_sql = f"""
        CREATE OR REPLACE VIEW active_available_stock AS
        SELECT item_code, {', '.join(case_statements)}
        FROM warehouse_dev.bin_table
        GROUP BY item_code;
    """

    # Step 3: Execute the dynamic SQL
    postgres_hook.run(dynamic_sql)

# Define the DAG
with DAG(
    dag_id="dynamic_query_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    dynamic_query_task = PythonOperator(
        task_id="generate_dynamic_query",
        python_callable=generate_query_and_execute
    )

    dynamic_query_task
