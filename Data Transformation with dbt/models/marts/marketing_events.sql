{{ config(
    materialized='incremental',
    schema='dev'
)
}}

SELECT 
    event_id,
    user_id,
    event_type,
    event_time,
    channel,
    campaign,
    cost,
    updated_at
FROM {{ref('stg_marketing_events')}} 

{% if is_incremental()%}
WHERE updated_at> (SELECT MAX(updated_at) FROM {{this}})
{% endif %}   