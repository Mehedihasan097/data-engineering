{{ config(materialized='view') }}

SELECT
    user_id,
    username,
    full_name,
    email,
    role,
    is_active,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'users') }}