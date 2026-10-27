{{ config(materialized='view') }}

SELECT
    branch_id,
    branch_name,
    address,
    city,
    country,
    contact_number,
    manager_user_id,
    CAST(opening_date AS DATE) AS opening_date,
    CAST(closing_date AS DATE) AS closing_date,
    is_active,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'branches') }}