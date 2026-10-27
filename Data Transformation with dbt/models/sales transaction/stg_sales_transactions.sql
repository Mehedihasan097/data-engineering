{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    transaction_id,
    product_id,
    user_id,
    transaction_timestamp::timestamp AS transaction_time,
    revenue::numeric AS revenue,
    cost::numeric AS cost,
    revenue - cost AS profit,
    updated_at
FROM {{ source('raw', 'sales_transactions') }}