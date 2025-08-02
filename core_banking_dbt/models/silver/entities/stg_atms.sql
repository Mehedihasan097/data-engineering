{{ config(materialized='view') }}

SELECT
    atm_id,
    location_name,
    address,
    city,
    country,
    atm_type,
    atm_status,
    cash_gl_account_id,
    branch_id,
    CAST(last_maintenance_date AS DATE) AS last_maintenance_date,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'atms') }}