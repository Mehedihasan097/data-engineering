{{ config(materialized='incremental', unique_key='ledger_entry_id') }}

with source_raw as (
    select
        ledger_entry_id,
        transaction_id,
        entry_sequence_no,
        transaction_timestamp,
        processing_timestamp,
        value_date,
        account_id,
        gl_account_code,
        amount,
        currency_code,
        entry_type,
        equivalent_base_amount,
        fx_rate,
        transaction_type_code,
        channel_code,
        processing_system_code,
        transaction_status_code,
        entry_description,
        batch_id,
        correlation_id,
        is_reversal_entry,
        reversed_ledger_entry_id,
        related_entity_type,
        related_entity_id,
        audit_user_id,
        audit_client_ip,
        audit_hash,
        created_date,
        last_updated_date
    from {{ ref('stg_financial_ledger') }}
    {% if is_incremental() %}
      where transaction_timestamp >= (
        select coalesce(max(transaction_timestamp), '1900-01-01'::timestamp)
        from {{ this }}
      )
    {% endif %}
),

ranked as (
    select
        *,
        row_number() over (
            partition by ledger_entry_id
            order by transaction_timestamp desc
        ) as rn
    from source_raw
)

select
    -- list all fields as needed; using the canonical names
    ledger_entry_id,
    transaction_id,
    entry_sequence_no,
    transaction_timestamp,
    processing_timestamp,
    value_date,
    account_id,
    gl_account_code,
    amount,
    currency_code,
    entry_type,
    equivalent_base_amount,
    fx_rate,
    transaction_type_code,
    channel_code,
    processing_system_code,
    transaction_status_code,
    entry_description,
    batch_id,
    correlation_id,
    is_reversal_entry,
    reversed_ledger_entry_id,
    related_entity_type,
    related_entity_id,
    audit_user_id,
    audit_client_ip,
    audit_hash,
    created_date,
    last_updated_date
from ranked
where rn = 1

