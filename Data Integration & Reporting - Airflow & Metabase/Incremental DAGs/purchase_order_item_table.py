from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
from psycopg2.extras import execute_values

# Function to extract data from MySQL and load it to PostgreSQL incrementally (last 7 days' data)
def transfer_data(**context):
    try:
        # Establish MySQL and PostgreSQL hooks
        mysql_hook = MySqlHook(mysql_conn_id='mysql_erpnext_prod')  # Replace with your MySQL connection ID
        postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse_dev')  # Replace with your PostgreSQL connection ID

        # Calculate the date range for the last 7 days and yesterday
        end_date = datetime.now().date()
        start_date = (datetime.now() - timedelta(days=7)).date()
        yesterday = (datetime.now() - timedelta(days=1)).date()

        # Extract data for the last 7 days from MySQL
        chunk_size = 10000
        offset = 0
        total_rows = 0
        last_day_rows = 0

        while True:
            # Extract chunk from MySQL for the last 7 days
            sql_query = f"""
            SELECT 
                name,
                item_code,
                ifw_retailskusuffix,
                barcode,
                material_request_item,
                supplier_part_no,
                creation,
                schedule_date,
                expected_delivery_date,
                warehouse,
                parent,
                parentfield,
                parenttype,
                image,
                item_name,
                description,
                brand,
                item_group,
                uom,
                docstatus,
                idx,
                apply_tds,
                last_purchase_rate,
                company_total_stock,
                stock_qty,
                qty,
                actual_qty,
                received_qty,
                returned_qty,
                discount_percentage,
                discount_amount,
                net_rate,
                base_net_rate,
                base_net_amount,
                net_amount,
                base_price_list_rate,
                price_list_rate,
                billed_amt,
                stock_uom,
                conversion_factor,
                material_request,
                delivered_by_supplier,
                is_free_item,
                expense_account,
                cost_center,
                ifw_location,
                stock_uom_rate,
                modified,
                modified_by,
                owner
            from erpnext_prod.`tabPurchase Order Item`
            WHERE DATE(creation) BETWEEN '{start_date}' AND '{end_date}'
               OR DATE(modified) BETWEEN '{start_date}' AND '{end_date}'
            LIMIT {chunk_size} OFFSET {offset};
            """
            logging.info(f"Executing MySQL query with offset {offset}, limit {chunk_size}, for date range {start_date} to {end_date}...")
            mysql_data = mysql_hook.get_pandas_df(sql_query)

            # If no more data is retrieved, break the loop
            if mysql_data.empty:
                logging.info("No more data to transfer. Completed.")
                break
            
            # Increment the offset for the next chunk
            offset += chunk_size
            total_rows += len(mysql_data)

            # Insert or update chunk in PostgreSQL
            try:
                # Use psycopg2's execute_values for efficient batch upsert
                upsert_query = f"""
                INSERT INTO warehouse_dev.purchase_order_item_table ({', '.join(mysql_data.columns)})
                VALUES %s
                ON CONFLICT (name) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in mysql_data.columns if col != 'name'])};
                """
                
                # Convert pandas DataFrame to list of tuples
                data_tuples = [tuple(row) for row in mysql_data.to_numpy()]
                
                # Get a connection from PostgresHook
                conn = postgres_hook.get_conn()
                cursor = conn.cursor()

                # Execute the upsert query using execute_values
                execute_values(cursor, upsert_query, data_tuples)
                conn.commit()

                logging.info(f"Successfully upserted {len(data_tuples)} rows.")

                # Check for yesterday's rows in target table for last_day_row_number
                if yesterday:
                    yesterday_sql = f"""
                    SELECT COUNT(*) FROM warehouse_dev.purchase_order_item_table
                    WHERE DATE(creation) = '{yesterday}';
                    """
                    yesterday_count = postgres_hook.get_first(yesterday_sql)[0]
                    last_day_rows = yesterday_count

            except Exception as insert_error:
                logging.error("Error while upserting data to PostgreSQL:", insert_error)
                raise

        # Get the start time and end time from context
        start_date_time = context['execution_date'].replace(tzinfo=None) 
        end_date_time = datetime.now()

        # Insert into the ETL tracking table
        etl_tracking_sql = """
        INSERT INTO warehouse_dev.etl_tracking (table_name, start_date_time, end_date_time, row_processed, last_day_row_number, status)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        status = 'success' if total_rows > 0 else 'failure'
        params = ('purchase_order_item_table', start_date_time, end_date_time, total_rows, last_day_rows, status)

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
    'tab_Purchase_Order_Item_ETL_Incremental',
    default_args=default_args,
    description='Incremental load of purchase order item data from MySQL to PostgreSQL',
    schedule_interval='32 10 * * *',  # Daily at 10:32 AM
    catchup=False,
)

# Define the PythonOperator
transfer_task = PythonOperator(
    task_id='transfer_tab_purchase_order_item_data_task_incremental',
    python_callable=transfer_data,
    provide_context=True,  # This will provide the context (including execution_date)
    dag=dag,
)

transfer_task