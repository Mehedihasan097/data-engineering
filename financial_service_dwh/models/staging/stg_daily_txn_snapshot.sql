{{
    config(
        materialized='table',
        post_hook=["CREATE INDEX IF NOT EXISTS idx_dm_txn_id ON {{ this }} (txn_id);"]
    )
}}

select
    txn_row_id,
    sequence_number,
    approval_timestamp,
    txn_date,
    txn_id,
    response_code,
    status,
    txn_type,
    processing_code,
    debit_credit_indicator,
    txn_category,
    channel_id,
    from_id,
    target_id,
    txn_amt,
    from_trust_level,
    target_trust_level
from {{ref('stg_financial_service_production_base_view')}}   
where txn_date = current_date - interval '6 day'