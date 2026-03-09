{{
    config(
        materialized='table'
    )
}}

WITH fraud_cases AS (
    SELECT
        t.trip_id,
        DATE(t.requested_at) AS fraud_date,
        c.city_name,
        d.driver_id,
        r.rider_id,
        t.gross_revenue,
        t.is_fraud_flag,
        
        -- Fraud reason breakdown
        CASE
            WHEN t.fraud_duplicate_payments THEN 'Duplicate Payment'
            WHEN t.fraud_failed_payment THEN 'Failed Payment on Completed Trip'
            WHEN t.fraud_extreme_surge THEN 'Extreme Surge (>10x)'
            WHEN t.fraud_no_payment THEN 'Completed Trip - No Payment'
            WHEN t.fraud_amount_mismatch THEN 'Amount Mismatch'
            ELSE 'Multiple Indicators'
        END AS fraud_reason,
        
        t.payment_method,
        t.trip_type
        
    FROM {{ ref('fact_trips') }} t
    JOIN {{ ref('dim_cities') }} c ON t.city_sk = c.city_sk
    JOIN {{ ref('dim_drivers') }} d ON t.driver_sk = d.driver_sk
    JOIN {{ ref('dim_riders') }} r ON t.rider_sk = r.rider_sk
    WHERE t.is_fraud_flag = TRUE
      AND t.trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
)

SELECT * FROM fraud_cases
ORDER BY fraud_date DESC, gross_revenue DESC