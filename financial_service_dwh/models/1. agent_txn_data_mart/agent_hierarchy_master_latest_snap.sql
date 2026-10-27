{{
    config(
        materialized='table'
    )
}}

with ranked_hierarchy as (
    select
        agent_id,
        agent_wallet,
        agent_name,
        dso_id,
        dso_wallet,
        dso_name,
        dh_id,
        dh_wallet,
        dh_name,
        region_name,
        tagging_timestamp,
        row_number() over (
            partition by agent_id 
            order by tagging_timestamp desc
        ) as rn
    from {{ ref('stg_agent_hierarchy_master_view') }}
)

select
    agent_id,
    agent_wallet,
    agent_name,
    dso_id,
    dso_wallet,
    dso_name,
    dh_id,
    dh_wallet,
    dh_name,
    region_name,
    tagging_timestamp
from ranked_hierarchy
where rn = 1