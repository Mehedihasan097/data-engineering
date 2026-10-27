from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging

# Function to extract data from MySQL and load it to PostgreSQL in chunks
def transfer_data():
    try:
        # Establish MySQL and PostgreSQL hooks
        mysql_hook = MySqlHook(mysql_conn_id='mysql_erpnext_prod')  # Replace with your MySQL connection ID
        postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse_dev')  # Replace with your PostgreSQL connection ID

        # Extract data in chunks using LIMIT and OFFSET
        chunk_size = 10000
        offset = 0
        total_rows = 0

        while True:
            # Extract chunk from MySQL
            sql_query = f"""
            SELECT 
                name,
                item_code,
                ifw_retailskusuffix,
                creation,
                item_name,
                description,
                item_group,
                brand,
                default_supplier,
                barcode,
                image,
                country_of_origin,
                has_variants,
                variant_of,
                variant_based_on,
                disabled,
                ifw_product_name_ci,
                ifw_discontinued,
                ifw_duty_rate,
                ifw_item_notes,
                ifw_po_notes,
                ifw_item_notes2,
                asi_item_class,
                lead_time_days,
                valuation_rate,
                is_sales_item,
                is_stock_item,
                is_purchase_item,
                max_discount,
                has_expiry_date,
                min_order_qty,
                last_purchase_rate,
                show_in_website,
                safety_stock,
                manufacturer_part_no,
                delivered_by_supplier,
                end_of_life,
                stock_uom,
                sales_uom,
                opening_stock,
                has_batch_no,
                standard_rate,
                shelf_life_in_days,
                include_item_in_manufacturing,
                no_of_months,
                no_of_months_exp,
                total_projected_qty,
                ifw_ismiscitem,
                over_delivery_receipt_allowance,
                over_billing_allowance,
                gpdeta_date,
                gpdorder_status,
                ais_month_cover,
                ais_poreorderqty,
                ais_poreorderlevel,
                gst_hsn_code,
                ifw_sku,
                ais_blockfrmstoresale,
                allow_negative_stock,
                tax_code,
                weight_uom,
                customs_tariff_number,
                allow_alternative_item,
                modified,
                modified_by,
                owner
            FROM erpnext_prod.tabItem
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
                    truncate_query = "TRUNCATE TABLE warehouse_dev.item_table;"
                    logging.info("Truncating PostgreSQL table...")
                    postgres_hook.run(truncate_query)
                    logging.info("Table truncated successfully.")

                # Use insert_rows method for batch insertion
                logging.info(f"Inserting {len(mysql_data)} rows into PostgreSQL...")
                postgres_hook.insert_rows(
                    table='warehouse_dev.item_table',
                    rows=mysql_data.values.tolist(),
                    target_fields=mysql_data.columns.tolist(),
                    commit_every=chunk_size
                )
                logging.info(f"Successfully inserted {len(mysql_data)} rows.")
            except Exception as insert_error:
                logging.error("Error while inserting data to PostgreSQL:", insert_error)
                raise

        logging.info(f"Data transfer completed successfully. Total rows transferred: {total_rows}")

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
    'tab_Item_Table_ETL',
    default_args=default_args,
    description='Transfer item data from MySQL to PostgreSQL daily in chunks',
    schedule_interval='0 1 * * *',  # Daily at 1:00 AM
    catchup=False,
)

# Define the PythonOperator
transfer_task = PythonOperator(
    task_id='transfer_tab_Item_data_task',
    python_callable=transfer_data,
    dag=dag,
)

transfer_task