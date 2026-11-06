from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
POSTGRES_CONN_ID = "postgres_default"

def notify_failure(context):
    """Sends a Telegram alert if any task in the pipeline fails."""
    try:
        hook = TelegramHook(telegram_conn_id='telegram_default')
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        exec_date = context.get('execution_date')
        
        message = (
            f"❌ *PIPELINE FAILURE ALERT*\n"
            f"Process: Top 10 Agents per DH Pipeline\n"
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

def check_source_data_mart():
    """Validation gate ensuring agent_txn_data_mart has data before processing Top 10."""
    check_table_row_count(
        table_name="agent_txn_data_mart",
        schema_name="data_mart",
        fail_message="No data found in source agent_txn_data_mart"
    )

def send_success_notification(**context):
    """Sends success notification with total row count of the built top_10_agents_per_dh model"""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = "SELECT count(*) FROM data_mart.top_10_agents_per_dh;"
    total_rows = hook.get_first(sql)[0]
    
    msg = (
        f"✅ *Top 10 Agents per DH Pipeline Successfully Completed!*\n"
        f"📊 Model `top_10_agents_per_dh` refreshed successfully.\n"
        f"📋 Total Rows in Model: `{total_rows:,}`"
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
    dag_id='top_10_agents_pipeline_dag',
    default_args=default_args,
    description='Orchestrates top_10_agents_per_dh dbt model with data validation and row-count Telegram notifications',
    schedule_interval='45 2 * * *',  # Runs daily at 2:45 AM (after main agent_txn_pipeline_dag)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['mfs', 'dbt', 'telegram', 'datamart', 'top10'],
) as dag:

    # Step 1: Start Notification
    notify_start = TelegramOperator(
        task_id='notify_top10_pipeline_started',
        telegram_conn_id='telegram_default',
        chat_id=TELEGRAM_CHAT_ID,
        text="🚀 *Top 10 Agents per DH Pipeline has been initiated.*",
        telegram_kwargs={'parse_mode': 'Markdown'},
    )

    # Step 2: Validate that source agent_txn_data_mart is populated and ready
    check_source_task = PythonOperator(
        task_id='check_agent_txn_data_mart_source',
        python_callable=check_source_data_mart,
    )

    # Step 3: Run top_10_agents_per_dh model via dbt
    run_top_10_model = BashOperator(
        task_id='run_dbt_top_10_agents_per_dh',
        bash_command='cd /opt/airflow/projects/financial_service_project/financial_service_dwh && dbt run --select top_10_agents_per_dh',
    )

    # Step 4: Success Notification with row count metrics
    notify_success = PythonOperator(
        task_id='notify_top10_pipeline_success',
        python_callable=send_success_notification,
    )

    # Pipeline Workflow Sequence
    notify_start >> check_source_task >> run_top_10_model >> notify_success