{{ config(
    materialized='view',
    schema='dev'
) }}

WITH sales AS(
    SELECT transaction_year,
           transaction_month,
           user_id,
           {{ sum_aggregate(['total_revenue', 'total_cost', 'total_profit'],['total_revenue', 'total_cost', 'total_profit']) }} 
    FROM    {{ ref('monthly_sales_report') }}
    {{group_by(3)}}
)

SELECT s.transaction_year,
       s.transaction_month,
       COUNT(s.user_id) AS unique_customers,
       {{sum_aggregate(
            ['total_revenue', 'total_cost', 'total_profit','total_marketing_events','total_marketing_cost']
        )}},
       CASE WHEN SUM(s.total_cost)>0 THEN SUM(s.total_revenue)/SUM(s.total_cost) ELSE 0 END AS revenue_to_cost_ratio
FROM   sales AS s
LEFT JOIN {{ref('monthly_event_report')}} m 
    ON s.user_id = m.user_id and s.transaction_year = m.event_year and s.transaction_month = m.event_month
{{group_by(2)}}
HAVING SUM(S.total_revenue) > 0
ORDER BY transaction_year, transaction_month   
