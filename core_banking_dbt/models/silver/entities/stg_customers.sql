{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_unique_id,
    first_name,
    last_name,
    CAST(date_of_birth AS DATE) AS date_of_birth,
    gender,
    contact_number,
    email_address,
    address_line1,
    city,
    country,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'customers') }}