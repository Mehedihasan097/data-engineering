{{ config(
    materialized='incremental', 
    schema='dev'
) }}

SELECT 
    t.transaction_id,
    t.product_id,
    p.product_name,
    p.category,
    t.user_id,
    t.transaction_time,
    t.revenue,
    t.cost,
    t.profit,
    t.updated_at
FROM {{ ref('stg_sales_transactions') }} t
LEFT JOIN {{ ref('stg_product_catalog') }} p ON t.product_id = p.product_id

{% if is_incremental() %}
WHERE t.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
