{{config(materialized='view')}}

SELECT
    currency_code,
    currency_name,
    decimal_places,
    symbol,
    is_active,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(updated_date AS TIMESTAMP) AS updated_date
FROM {{ source('core_banking', 'ref_currencies') }}