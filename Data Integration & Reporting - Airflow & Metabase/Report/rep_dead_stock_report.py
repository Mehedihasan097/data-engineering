from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def create_consolidated_view():
    # Connect to the database
    postgres_hook = PostgresHook(postgres_conn_id="postgres_warehouse_dev")
    
    # Query to fetch dynamic columns for active_available_stock (excluding item_code)
    stock_columns_query = """
    SELECT string_agg(column_name, ', ')
    FROM information_schema.columns
    WHERE table_name = 'active_available_stock' AND column_name != 'item_code';
    """
    stock_columns = postgres_hook.get_first(stock_columns_query)[0]

    # Query to fetch dynamic columns for last_12_month_sales_view (excluding item_code and total_sold)
    sales_columns_query = """
    SELECT string_agg(column_name, ', ')
    FROM information_schema.columns
    WHERE table_name = 'last_12_month_sales_view'
      AND column_name NOT IN ('item_code', 'SoldLast10Days', 'SoldLast30Days','SoldLast60Days','PreviousYearSold');
    """
    sales_columns = postgres_hook.get_first(sales_columns_query)[0]

    # Construct the dynamic SQL query
    create_view_query = f"""
    CREATE OR REPLACE VIEW consolidated_report AS
    SELECT 
        i.RetailSkuSuffix, 
        i.ERPNextItemCode, 
        i.ItemName,
        i.item_cost,
        i.standard_selling_rate,
        i.wholesale_rate,
        i.created_date,
        i.last_receive_date,
        i.default_supplier,
        i.supplier_part_no, 
        {stock_columns}, 
        {sales_columns},
        i.last_sales_date
    FROM 
        warehouse_dev.active_available_stock   a
    LEFT JOIN 
        warehouse_dev.item_details_report  i 
    ON 
        i.Item_Code = a.item_code
    LEFT JOIN 
        warehouse_dev.last_12_month_sales_view s
    ON 
        a.item_Code = s.item_code;
    """

    # Execute the query
    postgres_hook.run(create_view_query)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
}

with DAG(
    dag_id='rep_dead_stock_report_view',
    default_args=default_args,
    schedule_interval=None,  # Run manually or as per your requirement
    catchup=False,
) as dag:
    
    create_view_task = PythonOperator(
        task_id='dead_stock_report_view',
        python_callable=create_consolidated_view
    )

    create_view_task
