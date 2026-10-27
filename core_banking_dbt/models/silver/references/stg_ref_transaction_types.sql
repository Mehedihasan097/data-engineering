{{ config(materialized='view') }}

SELECT
    type_code,
    type_name,
    description,
    is_financial,
    is_reversal_type,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'ref_transaction_types') }}