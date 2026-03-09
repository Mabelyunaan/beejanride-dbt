{{
    config(
        materialized='table'
    )
}}

WITH payment_daily AS (
    SELECT
        DATE(p.created_at) AS payment_date,
        p.payment_provider,
        COUNT(*) AS total_payments,
        SUM(CASE WHEN p.payment_status = 'success' THEN 1 ELSE 0 END) AS successful_payments,
        SUM(CASE WHEN p.payment_status = 'failed' THEN 1 ELSE 0 END) AS failed_payments,
        
        -- Failure rate
        ROUND(
            SAFE_DIVIDE(
                SUM(CASE WHEN p.payment_status = 'failed' THEN 1 ELSE 0 END) * 100,
                COUNT(*)
            ), 2
        ) AS failure_rate_percentage,
        
        -- Amount analysis
        SUM(p.amount) AS total_amount,
        AVG(p.amount) AS avg_payment_amount
        
    FROM {{ ref('stg_payments') }} p
    GROUP BY 1, 2
)

SELECT * FROM payment_daily
ORDER BY payment_date DESC, failure_rate_percentage DESC