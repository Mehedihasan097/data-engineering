{{config(materialized= 'view')}}

select 
    "AGENT_ID" AS agent_id,
    "AGENT_WALLET" as agent_wallet,
    "AGENT_NAME" as agent_name,
    "DSO_ID" as dso_id,
    "DSO_WALLET"  as dso_wallet,
    "DSO_NAME" as dso_name,
    "DH_ID" as dh_id,
    "DH_WALLET" as dh_wallet,
    "DH_NAME" as dh_name,
    "REGION_NAME" as region_name,
    "TAGGING_TIMESTAMP" as tagging_timestamp
from {{ source('raw_prod', 'agent_hierarchy_master') }}    