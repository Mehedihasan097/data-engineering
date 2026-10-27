{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['txn_date', 'dh_wallet', 'agent_wallet'],
        schema='data_mart',
        on_schema_change='sync_all_columns'
    ) 
}}

with agent_daily_aggregates as (
    -- Step 1: Aggregate transactions per agent under each DH
    select 
        txn_date,
        region_name,
        dh_wallet,
        dh_name,
        dso_wallet,
        agent_wallet,
        count(*) as txn_cnt,
        sum(txn_amt) as txn_amt
    from {{ ref('agent_txn_data_mart') }}
    
    {% if is_incremental() %}
    where txn_date = current_date - interval '6 day'
    {% endif %}
    
    group by 
        txn_date, 
        region_name, 
        dh_wallet, 
        dh_name, 
        dso_wallet, 
        agent_wallet
),

ranked_agents as (
    -- Step 3: Assign rankings per DH using a window function
    select 
        txn_date,
        region_name,
        dh_wallet,
        dh_name,
        dso_wallet,
        agent_wallet,
        txn_cnt,
        txn_amt,
        row_number() over (
            partition by txn_date, dh_wallet 
            order by txn_amt desc, txn_cnt desc
        ) as rank_num
    from agent_daily_aggregates
)

-- Step 4: Filter strictly for the Top 10 per DH
select 
    txn_date,
    region_name,
    dh_wallet,
    dh_name,
    dso_wallet,
    agent_wallet,
    txn_cnt,
    txn_amt,
    rank_num
from ranked_agents
where rank_num <= 10