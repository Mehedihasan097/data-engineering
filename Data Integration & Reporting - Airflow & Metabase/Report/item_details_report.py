from datetime import datetime, timedelta
from airflow import DAG
#from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging

# Function to extract data from MySQL and load it to PostgreSQL in chunks
def transfer_data():
    try:
        # Establish MySQL and PostgreSQL hooks
        #mysql_hook = MySqlHook(mysql_conn_id='mysql_erpnext_dev')  # Replace with your MySQL connection ID
        postgres_hook = PostgresHook(postgres_conn_id='postgres_warehouse_dev')  # Replace with your PostgreSQL connection ID

        # Extract data in chunks using LIMIT and OFFSET
        chunk_size = 10000
        offset = 0
        total_rows = 0

        while True:
            # Extract chunk from MySQL
            sql_query = f"""
            WITH last_receive AS(
            SELECT 
                    c.item_code,
                    MAX(p.transaction_date) AS last_receive_date
                FROM warehouse_dev.purchase_order_table p     
                INNER JOIN warehouse_dev.purchase_order_item_table c 
                    ON p.name = c.parent
                WHERE p.docstatus = 1
                    AND (c.warehouse IS NULL OR c.warehouse <> 'US02-Houston - Active Stock - ICL')
                GROUP BY c.item_code
            ),

            last_sold as(
            SELECT 
                c.item_code, 
                max(p.posting_date) last_sales_date
            FROM 
            warehouse_dev.sales_invoice_item_table c
            LEFT JOIN 
            warehouse_dev.sales_invoice_table p ON p.name = c.parent
            WHERE 
            p.docstatus = 1
            AND (c.warehouse IS NULL OR c.warehouse <> 'US02-Houston - Active Stock - ICL')
            group by c.item_code
            ),

            selling_rate AS(
            select item_code,
                price_list_rate selling_rate
            from(       
                select item_code,
                price_list_rate,
                ROW_NUMBER() OVER (
                        PARTITION BY item_code 
                        ORDER BY 
                            CASE 
                                WHEN current_date BETWEEN valid_from AND valid_upto THEN 1
                                WHEN valid_from <= current_date OR valid_upto >= current_date THEN 2
                                WHEN valid_from IS NULL AND valid_upto IS NULL THEN 3
                                ELSE 4
                            END
                    ) AS row_num
                from  warehouse_dev.item_price_table 
                where 1=1
                and   selling = 1 
                )
            where row_num =1    
            ),

            item_cost AS(
            select item_code,
                price_list_rate item_cost  
            from(
                select item_code,
                price_list_rate,
                ROW_NUMBER() OVER (
                        PARTITION BY item_code 
                        ORDER BY 
                            CASE 
                                WHEN current_date BETWEEN valid_from AND valid_upto THEN 1
                                WHEN valid_from <= current_date OR valid_upto >= current_date THEN 2
                                WHEN valid_from IS NULL AND valid_upto IS NULL THEN 3
                                ELSE 4
                            END
                    ) AS row_num
                from  warehouse_dev.item_price_table 
                where 1=1
                and   buying = 1 
            ) 
            where row_num =1
            ),

            standard_selling_rate AS(
            select item_code,
                price_list_rate standard_selling_rate
            from(
            select item_code,
                price_list_rate,
                ROW_NUMBER() OVER (PARTITION BY item_code ORDER BY valid_from desc) AS row_num
            from  warehouse_dev.item_price_table 
            where 1=1
            and   selling = 1 
            and price_list = 'RET - Camo'

            )
            where row_num =1
            )


            select 
                    a.ifw_retailskusuffix AS retailskusuffix,
                    a.item_code AS ERPNextItemCode,
                    case when lr.last_receive_date is null and ls.last_sales_date is null then 'No Receive, No Sale'
                        when ls.last_sales_date is not null then 'have sale' end receive_sale_class,
                    date(a.creation) created_date,
                    lr.last_receive_date,
                    ls.last_sales_date,
                    s.supplier,
                    s.supplier_part_no,
                    a.barcode,
                    a.item_name,
                    a.item_group,
                    a.image AS item_image,
                    a.ifw_product_name_ci AS product_name_ci,
                    ic.item_cost,
                    sr.selling_rate,
                    ssr.standard_selling_rate,
                    a.country_of_origin,
                    a.gst_hsn_code AS hsn_code,
                    a.default_supplier,
                    a.ifw_item_notes AS item_notes,
                    a.ifw_item_notes2 AS item_notes2,
                    a.ifw_po_notes AS po_notes,
                    a.ifw_discontinued
            FROM warehouse_dev.item_table  a
            inner join warehouse_dev.supplier_item_table s on a.name = s.parent
            left join last_receive lr on s.parent = lr.item_code
            left join last_sold ls on s.parent = ls.item_code
            left join selling_rate sr on s.parent = sr.item_code
            left join item_cost ic on s.parent = ic.item_code
            left join standard_selling_rate ssr on s.parent = ssr.item_code

            LIMIT {chunk_size} OFFSET {offset};
            """
            logging.info(f"Executing SQL query with offset {offset} and limit {chunk_size}...")
            sql_data = postgres_hook.get_pandas_df(sql_query)

            # If no more data is retrieved, break the loop
            if sql_data.empty:
                logging.info("No more data to transfer. Completed.")
                break
            
            # Increment the offset for the next chunk
            offset += chunk_size
            total_rows += len(sql_data)

            # Insert chunk into PostgreSQL
            try:
                # Truncate target table only on the first iteration
                if offset == chunk_size:
                    truncate_query = "TRUNCATE TABLE warehouse_dev.item_details_report;"
                    logging.info("Truncating PostgreSQL table...")
                    postgres_hook.run(truncate_query)
                    logging.info("Table truncated successfully.")

                # Use insert_rows method for batch insertion
                logging.info(f"Inserting {len(sql_data)} rows into PostgreSQL...")
                postgres_hook.insert_rows(
                    table='warehouse_dev.item_details_report',
                    rows=sql_data.values.tolist(),
                    target_fields=sql_data.columns.tolist(),
                    commit_every=chunk_size
                )
                logging.info(f"Successfully inserted {len(sql_data)} rows.")
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
    'Rep_Item_Details_Report_ETL',
    default_args=default_args,
    description='Generate Item Details Report daily in chunks',
    schedule_interval='0 1 * * *',  # Daily at 1:00 AM
    catchup=False,
)

# Define the PythonOperator
transfer_task = PythonOperator(
    task_id='transfer_Rep_Item_Details_Report_data_task',
    python_callable=transfer_data,
    dag=dag,
)

transfer_task