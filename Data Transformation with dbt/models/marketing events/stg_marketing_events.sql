{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT 
    event_id,
    user_id,
    event_type,
    event_timestamp::timestamp AS event_time,
    channel,
    campaign,
    cost::numeric AS cost,
    updated_at
FROM {{ source('raw', 'marketing_events') }}