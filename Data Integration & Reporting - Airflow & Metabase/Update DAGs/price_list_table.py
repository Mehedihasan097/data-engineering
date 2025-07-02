from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging

def transfer_data(**context):
    try:
        # Establish MySQL and PostgreSQL hooks
        mysql_hook = MySqlHook(mysql_conn_id='mysql_erpnext_prod')  # Replace with your MySQL connection ID
        postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse_dev')  # Replace with your PostgreSQL connection ID

        # Extract data in chunks using LIMIT and OFFSET
        chunk_size = 10000
        offset = 0
        total_rows = 0

        # Get start time for ETL tracking
        start_date_time = context['execution_date'].replace(tzinfo=None)

        while True:
            # Extract chunk from MySQL
            sql_query = f"""
            SELECT 
                name,
                creation,
                modified,
                price_list_name,
                currency,
                selling,
                buying,
                enabled,
                modified_by,
                owner
            from erpnext_prod.`tabPrice List`
            LIMIT {chunk_size} OFFSET {offset};
            """
            logging.info(f"Executing MySQL query with offset {offset} and limit {chunk_size}...")
            mysql_data = mysql_hook.get_pandas_df(sql_query)

            # If no more data is retrieved, break the loop
            if mysql_data.empty:
                logging.info("No more data to transfer. Completed.")
                break

            # Increment the offset for the next chunk
            offset += chunk_size
            total_rows += len(mysql_data)

            # Insert chunk into PostgreSQL
            try:
                # Truncate target table only on the first iteration
                if offset == chunk_size:
                    truncate_query = "TRUNCATE TABLE warehouse_dev.price_list_table;"
                    logging.info("Truncating PostgreSQL table...")
                    postgres_hook.run(truncate_query)
                    logging.info("Table truncated successfully.")

                # Use insert_rows method for batch insertion
                logging.info(f"Inserting {len(mysql_data)} rows into PostgreSQL...")
                postgres_hook.insert_rows(
                    table='warehouse_dev.price_list_table',
                    rows=mysql_data.values.tolist(),
                    target_fields=mysql_data.columns.tolist(),
                    commit_every=chunk_size
                )
                logging.info(f"Successfully inserted {len(mysql_data)} rows.")
            except Exception as insert_error:
                logging.error("Error while inserting data to PostgreSQL:", insert_error)
                raise

        # Get end time for ETL tracking
        end_date_time = datetime.now()

        # Insert into the ETL tracking table
        etl_tracking_sql = """
        INSERT INTO warehouse_dev.etl_tracking (table_name, start_date_time, end_date_time, row_processed, last_day_row_number, status)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        status = 'success' if total_rows > 0 else 'failure'
        last_day_rows = total_rows  # For full table transfers, last_day_rows equals total_rows
        params = ('price_list_table', start_date_time, end_date_time, total_rows, last_day_rows, status)

        postgres_hook.run(etl_tracking_sql, parameters=params)

        logging.info(f"ETL tracking record inserted. Total rows processed: {total_rows}, Last day rows: {last_day_rows}")

    except Exception as e:
        logging.error("Error occurred during data transfer:", e)
        raise

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 24),  # Adjust to today's or intended start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tab_price_list_table_ETL',
    default_args=default_args,
    description='Transfer price list data from MySQL to PostgreSQL daily in chunks with ETL tracking',
    schedule_interval='10 15 * * *',  # Daily at 10:15 AM
    catchup=False,
)

# Define the PythonOperator
transfer_task = PythonOperator(
    task_id='transfer_tab_price_list_data_task',
    python_callable=transfer_data,
    provide_context=True,
    dag=dag,
)

transfer_task
