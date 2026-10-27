{{ config(
    materialized='view',
    schema='dev'
) }}

SELECT {{get_date_parts('event_time', 'event_year', 'event_quarter', 'event_month')}},
       user_id,
       COUNT(event_id) AS total_marketing_events,
       {{sum_aggregate(['cost'],['total_marketing_cost'])}}
FROM  {{ref('marketing_events')}}      
{{group_by(4)}}       