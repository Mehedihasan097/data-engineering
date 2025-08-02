{{config(materialized='view')}}

SELECT
    system_code,
    system_name,
    description,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'ref_processing_systems') }}