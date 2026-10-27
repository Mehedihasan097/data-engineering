{{config(
    materialized= 'view',
    schema = 'dbt_staging'
    )}}


select
    "ID" as txn_row_id,
    "SQN" as sequence_number,
    
    -- Convert string timestamp to proper PostgreSQL timestamp
    to_timestamp("APPROVAL_DATETIME", 'DD-Mon-YY HH12.MI.SS.US AM') as approval_timestamp,
    cast(to_timestamp("APPROVAL_DATETIME", 'DD-Mon-YY HH12.MI.SS.US AM') as date) as txn_date,
    
    "TXN_ID" as txn_id,
    "RESPONSE_CODE" as response_code,
    "STATUS" as status,
    "TXN_TYPE" as txn_type,
    "PROCESSING_CODE" as processing_code,
    "TXN_TYPE_D_C" as debit_credit_indicator,
    "TXN_CAT" as txn_category,
    "CHANNEL" as channel_id,
    "FROM_ID" as from_id,
    "TARGET_ID" as target_id,
    "TXN_AMT" as txn_amt,
    "FROM_TRUST_LEVEL" as from_trust_level,
    "TARGET_TRUST_LEVEL" as target_trust_level

from {{ source('raw_prod', 'financial_txn_production_base') }}