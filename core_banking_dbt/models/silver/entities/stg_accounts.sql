{{ config(materialized='view') }}

SELECT
    account_id,
    account_number,
    customer_id,
    account_type,
    currency_code,
    current_balance,
    CAST(opening_date AS DATE) AS opening_date,
    CAST(closing_date AS DATE) AS closing_date,
    account_status,
    gl_account_code,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'accounts') }}