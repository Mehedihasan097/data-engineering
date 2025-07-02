{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    product_id,
    product_name,
    category
FROM {{ source('raw', 'product_catalog') }}