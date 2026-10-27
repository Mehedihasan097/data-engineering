{{config(materialized='view')}}

SELECT
    gl_code,
    gl_name,
    account_type,
    parent_gl_code,
    is_active,
    CAST(created_date AS TIMESTAMP) AS created_date,
    CAST(last_updated_date AS TIMESTAMP) AS last_updated_date
FROM {{ source('core_banking', 'ref_gl_accounts') }}