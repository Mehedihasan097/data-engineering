{{ config(
    materialized='incremental',   
    schema='dev'
) }}

SELECT 
    product_id,
    product_name,
    category
FROM {{ ref('stg_product_catalog') }}

{% if is_incremental() %}
-- Only insert new products
WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}
