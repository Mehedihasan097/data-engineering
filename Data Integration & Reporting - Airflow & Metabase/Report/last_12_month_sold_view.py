from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def generate_last_12_months_view(**kwargs):
    # Initialize the PostgresHook
    postgres_hook = PostgresHook(postgres_conn_id="postgres_warehouse_dev")

    # Step 1: Generate dynamic column names for the last 12 months
    today = datetime.today()
    months = [(today - timedelta(days=30 * i)).strftime("%b_%Y_Sold") for i in range(12)]

    # Generate CASE statements for each month
    case_statements = []
    for i, month in enumerate(months):
        start_date = (today - timedelta(days=30 * (i + 1))).replace(day=1).strftime('%Y-%m-%d')
        end_date = (today - timedelta(days=30 * i)).replace(day=1).strftime('%Y-%m-%d')
        case_statements.append(
            f"SUM(CASE WHEN p.posting_date >= '{start_date}' AND p.posting_date < '{end_date}' THEN c.qty ELSE 0 END) AS \"{month}\""
        )

    # Step 2: Construct the SQL for creating the view
    dynamic_sql = f"""
        CREATE OR REPLACE VIEW warehouse_dev.last_12_months_sales AS
        SELECT
            c.item_code,
            {', '.join(case_statements)}
        FROM
            sales_invoice_item_table c
        LEFT JOIN
            sales_invoice_table p ON p.name = c.parent
        WHERE
            p.docstatus = 1
            AND (c.warehouse IS NULL OR c.warehouse <> 'US02-Houston - Active Stock - ICL')
        GROUP BY
            c.item_code;
    """

    # Step 3: Execute the dynamic SQL
    postgres_hook.run(dynamic_sql)

# Define the DAG
with DAG(
    dag_id="create_last_12_months_sales_view",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_view_task = PythonOperator(
        task_id="generate_last_12_months_view",
        python_callable=generate_last_12_months_view
    )

    create_view_task
