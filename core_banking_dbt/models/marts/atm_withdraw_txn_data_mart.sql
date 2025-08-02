{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='sync'
) }}

WITH silver_ledger AS (
    SELECT *
    FROM {{ ref('financial_ledger_deduped') }}
    WHERE channel_code = '100'
),

debit_entries AS (
    SELECT
        transaction_id,
        transaction_timestamp,
        account_id AS customer_account_id,
        amount,
        transaction_type_code AS type_code,
        channel_code
    FROM silver_ledger
    WHERE entry_type = 'DR'
      AND transaction_type_code = '001'
      AND processing_system_code = '200'
),

credit_entries AS (
    SELECT
        transaction_id,
        transaction_timestamp,
        account_id AS atm_account_id,
        amount
    FROM silver_ledger
    WHERE entry_type = 'CR'
      AND transaction_type_code = '001'
      AND processing_system_code = '201'
),

atm_service_fee AS (
    SELECT
        transaction_id,
        transaction_timestamp,
        amount AS atm_service_fee
    FROM silver_ledger
    WHERE entry_type = 'CR'
      AND transaction_type_code = '021'
      AND processing_system_code = '206'
),

joined_txn AS (
    SELECT
        dr.transaction_id,
        dr.transaction_timestamp,
        dr.customer_account_id,
        cr.atm_account_id,
        dr.type_code,
        dr.channel_code,
        dr.amount AS withdrawal_amount,
        sf.atm_service_fee
    FROM debit_entries dr
    LEFT JOIN credit_entries cr
        ON dr.transaction_id = cr.transaction_id
    LEFT JOIN atm_service_fee sf
        ON dr.transaction_id = sf.transaction_id
),

final_mart AS (
    SELECT
        jt.transaction_id,
        jt.transaction_timestamp,
        jt.customer_account_id,
        jt.atm_account_id,
        atm.atm_id,
        atm.location_name,
        br.branch_id,
        br.branch_name,
        rtt.type_name,
        jt.channel_code,
        rc.channel_name,
        jt.withdrawal_amount,
        jt.atm_service_fee
    FROM joined_txn jt
    LEFT JOIN {{ ref('stg_atms') }} atm
        ON jt.atm_account_id = atm.cash_gl_account_id
    LEFT JOIN {{ ref('stg_branches') }} br
        ON atm.branch_id = br.branch_id
    LEFT JOIN {{ ref('stg_ref_transaction_types') }} rtt
        ON jt.type_code = rtt.type_code
    LEFT JOIN {{ ref('stg_ref_channels') }} rc
        ON jt.channel_code = rc.channel_code
)

SELECT * FROM final_mart

{% if is_incremental() %}
WHERE transaction_timestamp > (SELECT MAX(transaction_timestamp) FROM {{ this }})
{% endif %}
