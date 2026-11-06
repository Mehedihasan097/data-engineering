import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import your shared custom email utility function
from email_utils import send_custom_email

TELEGRAM_CHAT_ID = Variable.get("telegram_chat_id")
POSTGRES_CONN_ID = "postgres_default"

def notify_failure(context):
    """Sends a Telegram alert if any step in the report circulation fails."""
    try:
        hook = TelegramHook(telegram_conn_id='telegram_default')
        dag_id = context.get('dag').dag_id
        task_id = context.get('task_instance').task_id
        exec_date = context.get('execution_date')
        
        message = (
            f"❌ *REPORT CIRCULATION FAILURE*\n"
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

def extract_data_and_save_excel(**context):
    """Step 1: Queries yesterday's top 10 report data and exports to local Excel."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    sql = """
        SELECT 
            txn_date,
            region_name,
            dh_wallet,
            dh_name,
            dso_wallet,
            agent_wallet,
            txn_cnt,
            txn_amt,
            rank_num
        FROM data_mart.top_10_agents_per_dh
        WHERE txn_date = current_date - interval '1 day'
        ORDER BY dh_name, rank_num ASC;
    """
    df = hook.get_pandas_df(sql)
    
    if df.empty:
        print("⚠️ No records found for yesterday's top 10 report.")
        raise ValueError("Top 10 report data source is empty for yesterday.")

    # Define local server storage path
    local_dir = "/opt/airflow/projects/financial_service_project/reports"
    os.makedirs(local_dir, exist_ok=True)
    
    exec_date_str = context.get('execution_date').strftime('%Y-%m-%d')
    file_name = f"top_10_agents_per_dh_{exec_date_str}.xlsx"
    file_path = os.path.join(local_dir, file_name)
    
    # Export DataFrame to local Excel file
    df.to_excel(file_path, index=False, engine='openpyxl')
    print(f"✅ Excel report successfully saved locally at: {file_path}")
    
    # Push file path to XCom for downstream tasks to consume
    context['task_instance'].xcom_push(key='excel_file_path', value=file_path)

def notify_extraction_success(**context):
    """Step 2: Notify via Telegram that data has been successfully extracted into Excel."""
    exec_date_str = context.get('execution_date').strftime('%Y-%m-%d')
    msg = (
        f"📋 *Data Extraction Complete*\n"
        f"📊 Top 10 Agents report for `{exec_date_str}` has been successfully extracted and saved to server storage."
    )
    hook = TelegramHook(telegram_conn_id='telegram_default')
    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': msg,
        'parse_mode': 'Markdown'
    })

def send_email_report(**context):
    """Step 3: Retrieve local Excel file path from XCom and send via email_utils."""
    ti = context['task_instance']
    file_path = ti.xcom_pull(task_ids='extract_data_to_excel', key='excel_file_path')
    exec_date_str = context.get('execution_date').strftime('%Y-%m-%d')

    html_body = f"""
    <h3>📊 Daily Top 10 Agents per Distributor House Report</h3>
    <p>Execution Date: <b>{exec_date_str}</b></p>
    <p>Please find attached the Excel report containing yesterday's top-performing agents, transaction counts, and volume totals.</p>
    <br>
    <p><i>Generated automatically by MFS Data Engineering Pipeline.</i></p>
    """

    send_custom_email(
        from_email="---@gmail.com",
        to_emails=["---@gmail.com"],
        cc_emails=["--@gmail.com"],
        subject=f"📊 Top 10 Agents Report - {exec_date_str}",
        html_content=html_body,
        attachment_path=file_path
    )

def notify_email_success(**context):
    """Step 4: Final Telegram notification confirming successful email circulation."""
    exec_date_str = context.get('execution_date').strftime('%Y-%m-%d')
    msg = (
        f"✅ *Email Circulation Successful!*\n"
        f"📧 The Top 10 Agents report for `{exec_date_str}` has been successfully emailed to all stakeholders."
    )
    hook = TelegramHook(telegram_conn_id='telegram_default')
    hook.send_message({
        'chat_id': TELEGRAM_CHAT_ID,
        'text': msg,
        'parse_mode': 'Markdown'
    })

default_args = {
    'owner': 'mfs_data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': notify_failure,
}

with DAG(
    dag_id='report_circulation_pipeline_dag',
    default_args=default_args,
    description='Extracts top 10 report to Excel, notifies via Telegram, emails stakeholders, and confirms completion',
    schedule_interval='0 3 * * *',  # Runs daily at 3:00 AM
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['mfs', 'reports', 'email', 'excel', 'circulation'],
) as dag:

    # Task 1: Extract data and save to Excel
    extract_task = PythonOperator(
        task_id='extract_data_to_excel',
        python_callable=extract_data_and_save_excel,
        provide_context=True,
    )

    # Task 2: Notify extraction on Telegram
    notify_extraction_task = PythonOperator(
        task_id='notify_extraction_success',
        python_callable=notify_extraction_success,
        provide_context=True,
    )

    # Task 3: Send email with attachment using email_utils
    email_task = PythonOperator(
        task_id='send_email_report',
        python_callable=send_email_report,
        provide_context=True,
    )

    # Task 4: Notify final success on Telegram
    notify_email_success_task = PythonOperator(
        task_id='notify_email_success',
        python_callable=notify_email_success,
        provide_context=True,
    )

    # Strict Sequential Pipeline Workflow Sequence
    extract_task >> notify_extraction_task >> email_task >> notify_email_success_task