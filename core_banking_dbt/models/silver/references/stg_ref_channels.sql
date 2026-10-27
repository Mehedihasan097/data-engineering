{{ config(materialized='view')}}

SELECT
    channel_code,
    channel_name,
    description,
    is_active,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'ref_channels') }}