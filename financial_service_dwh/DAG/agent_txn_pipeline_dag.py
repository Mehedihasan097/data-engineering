from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
POSTGRES_CONN_ID = "postgres_default"  # Update if your Airflow postgres connection ID differs

def notify_failure(context):
    """Sends a Telegram alert if any task in the pipeline fails."""
    try:
        hook = TelegramHook(telegram_conn_id='telegram_default')
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        exec_date = context.get('execution_date')
        
        message = (
            f"❌ *PIPELINE FAILURE ALERT*\n"
            f"Process: Agent Txn Data Mart Pipeline\n"
            f"DAG: `{dag_id}`\n"
            f"Failed Task: `{task_id}`\n"
            f"Time: {exec_date}"
        )
        hook.send_message({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message, 
            'parse_mode': 'Markdown'
        })
    except Exception as e:
        print(f"Failed to send Telegram failure notification: {e}")

def check_table_row_count(table_name, schema_name, fail_message):
    """Checks if a table has records. Fails task and alerts instantly if count is 0."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = f"SELECT count(*) FROM {schema_name}.{table_name};"
    row_count = hook.get_first(sql)[0]
    
    print(f"Row count for {schema_name}.{table_name}: {row_count}")
    
    if row_count is None or row_count == 0:
        alert_msg = f"❌ *Validation Error*: `{fail_message}` (Table `{schema_name}.{table_name}` has `0` rows)."
        try:
            tg_hook = TelegramHook(telegram_conn_id='telegram_default')
            tg_hook.send_message({
                'chat_id': TELEGRAM_CHAT_ID,
                'text': alert_msg,
                'parse_mode': 'Markdown'
            })
        except Exception as e:
            print(f"Failed to send instant validation Telegram alert: {e}")
        raise ValueError(fail_message)
    
    return row_count

def check_stg_daily_txn():
    check_table_row_count(
        table_name="stg_daily_txn_snapshot",
        schema_name="dbt_staging",
        fail_message="No data found in stg_daily_txn_snapshot"
    )

def check_hierarchy_master():
    check_table_row_count(
        table_name="agent_hierarchy_master_latest_snap",
        schema_name="dbt_staging",
        fail_message="No data found in agent_hierarchy_master_latest_snap"
    )

def send_success_notification(**context):
    """Sends success notification with total row count of the built datamart"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = "SELECT count(*) FROM data_mart.agent_txn_data_mart;"
    total_rows = hook.get_first(sql)[0]
    
    msg = (
        f"✅ *Agent Txn Data Mart Pipeline Successfully Completed!*\n"
        f"📊 `agent_txn_data_mart` & `top_10_agents_per_dh` models refreshed.\n"
        f"📋 Total Rows in Data Mart: `{total_rows:,}`"
    )
    try:
        tg_hook = TelegramHook(telegram_conn_id='telegram_default')
        tg_hook.send_message({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': msg,
            'parse_mode': 'Markdown'
        })
    except Exception as e:
        print(f"Failed to send success Telegram message: {e}")

default_args = {
    'owner': 'mfs_data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id='agent_txn_pipeline_dag',
    default_args=default_args,
    description='Orchestrates agent transaction data mart pipeline with validation gates and Telegram notifications',
    schedule_interval='30 2 * * *',  # Runs daily at 2:30 AM (after snapshot pipeline completes)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['mfs', 'dbt', 'telegram', 'datamart'],
) as dag:

    # Step 1: Start Notification
    notify_start = TelegramOperator(
        task_id='notify_pipeline_started',
        telegram_conn_id='telegram_default',
        chat_id=TELEGRAM_CHAT_ID,
        text="🚀 *Agent Txn Data Mart Pipeline has been initiated.*",
        telegram_kwargs={'parse_mode': 'Markdown'},
    )

    # Step 2: Check data availability on stg_daily_txn_snapshot
    check_stg_snapshot = PythonOperator(
        task_id='check_stg_daily_txn_snapshot',
        python_callable=check_stg_daily_txn,
    )

    # Step 3: Run agent_hierarchy_master_latest_snap via dbt
    run_hierarchy_model = BashOperator(
        task_id='run_agent_hierarchy_master_latest_snap',
        bash_command='cd /opt/airflow/projects/financial_service_project/financial_service_dwh && dbt run --select agent_hierarchy_master_latest_snap',
    )

    # Step 4: Check data availability on agent_hierarchy_master_latest_snap
    check_hierarchy_model = PythonOperator(
        task_id='check_agent_hierarchy_master_latest_snap',
        python_callable=check_hierarchy_master,
    )

    # Step 5: Run agent_txn_data_mart and top_10_agents_per_dh models via dbt
    run_data_marts = BashOperator(
        task_id='run_agent_txn_data_marts',
        bash_command='cd /opt/airflow/projects/financial_service_project/financial_service_dwh && dbt run --select agent_txn_data_mart top_10_agents_per_dh',
    )

    # Step 6: Success Notification with row count metrics
    notify_success = PythonOperator(
        task_id='notify_pipeline_success',
        python_callable=send_success_notification,
    )

    # Pipeline Workflow Sequence
    notify_start >> check_stg_snapshot >> run_hierarchy_model >> check_hierarchy_model >> run_data_marts >> notify_success