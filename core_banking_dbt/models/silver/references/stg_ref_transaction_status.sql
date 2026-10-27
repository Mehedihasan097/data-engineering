{{config(materialized='view')}}

SELECT
    status_code,
    status_name,
    description
FROM {{ source('core_banking', 'ref_transaction_status') }}