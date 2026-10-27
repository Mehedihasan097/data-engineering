{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='txn_id',
        schema='data_mart',
        on_schema_change='sync_all_columns'
        
    )
}}

with day_txns as (
    -- Reads whatever single day of data is currently in your daily snapshot table
    select 
        approval_timestamp,
        txn_date,
        txn_id,
        txn_type,
        processing_code,
        txn_category,
        from_id,
        target_id,
        txn_amt
    from {{ ref('stg_daily_txn_snapshot') }}
    where txn_type = 53
      and processing_code = '530085'
),

customer_debit as (
    select 
        approval_timestamp,
        txn_date,
        txn_id,
        'cash withdraw' as txn_tpe,
        from_id as customer_id,
        target_id as agent_id,
        txn_amt
    from day_txns
    where txn_category = 12
),

service_fee as (
    select 
        txn_id,
        txn_amt as service_fee
    from day_txns
    where txn_category = 91
),

agent_commission as (
    select 
        txn_id,
        txn_amt as agent_commission
    from day_txns
    where txn_category = 92
)

select 
    a.approval_timestamp,
    a.txn_date,
    a.txn_id,
    a.txn_tpe as txn_type,
    d.agent_wallet,
    d.agent_name,
    d.dso_wallet,
    d.dh_wallet,
    d.dh_name,
    d.region_name,
    a.agent_id::text as agent_id,
    a.customer_id::text as customer_id,
    a.txn_amt,
    coalesce(b.service_fee, 0) as service_fee,
    coalesce(c.agent_commission, 0) as agent_commission
from customer_debit a
left join service_fee b on a.txn_id = b.txn_id
left join agent_commission c on a.txn_id = c.txn_id
left join {{ ref('agent_hierarchy_master_latest_snap') }} d 
    on a.agent_id::text = d.agent_id::text