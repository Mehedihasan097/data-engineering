{{ config(
    materialized='view',
    schema='dev'
) }}

SELECT 
    {{ get_date_parts('transaction_time', 'transaction_year', 'transaction_quarter', 'transaction_month') }},
    user_id,
    category as product_category,
    {{ sum_aggregate(['revenue', 'cost', 'profit'],  ['total_revenue', 'total_cost', 'total_profit']) }} 
FROM {{ref('sales_transactions')}}
{{ group_by(5) }}  

