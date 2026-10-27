from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook


TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")

def notify_failure(context):
    """Sends a Telegram alert if any task in the pipeline fails."""
    try:
        hook = TelegramHook(telegram_conn_id='telegram_default')
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        exec_date = context.get('execution_date')
        
        message = (
            f"❌ *PIPELINE FAILURE ALERT*\n"
            f"Process: Daily Txn Snapshot\n"
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

default_args = {
    'owner': 'mfs_data_engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id='financial_dwh_daily_snapshot_pipeline',
    default_args=default_args,
    description='Orchestrates dbt daily transaction snapshot with Telegram notifications',
    schedule_interval='0 0 * * *',  # Triggers daily at 12:00 AM midnight
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['mfs', 'dbt', 'telegram', 'snapshot'],
) as dag:

    # Step 1: Start Notification
    notify_start = TelegramOperator(
        task_id='notify_snapshot_started',
        telegram_conn_id='telegram_default',
        chat_id=TELEGRAM_CHAT_ID,
        text="🚀 *Daily Txn Snapshot has been initiated.*",
        telegram_kwargs={'parse_mode': 'Markdown'},
    )

    # Step 2: Execute dbt Snapshot Model via Mounted Volume Path
    run_dbt_snapshot = BashOperator(
        task_id='run_stg_daily_txn_snapshot',
        bash_command='cd /opt/airflow/projects/financial_service_project/financial_service_dwh && dbt run --select stg_daily_txn_snapshot',
    )

    # Step 3: Success Notification
    notify_success = TelegramOperator(
        task_id='notify_snapshot_success',
        telegram_conn_id='telegram_default',
        chat_id=TELEGRAM_CHAT_ID,
        text="✅ *The Daily Txn Snapshot has been successfully Completed.* Data is audited and ready.",
        telegram_kwargs={'parse_mode': 'Markdown'},
    )

    # Pipeline Workflow Sequence
    notify_start >> run_dbt_snapshot >> notify_success