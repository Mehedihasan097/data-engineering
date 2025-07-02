{% snapshot snapshot_marketing_events %}

{{ config(
    target_schema='snapshots',
    unique_key='event_id',
    strategy='check',
    check_cols=['cost']
) }}

SELECT * FROM {{ source('raw', 'marketing_events') }}

{% endsnapshot %}