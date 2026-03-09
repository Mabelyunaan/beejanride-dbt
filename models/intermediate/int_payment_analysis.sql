{{
    config(
        materialized='table'
    )
}}

WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

trips AS (
    SELECT 
        trip_id,
        trip_status,
        actual_fare
    FROM {{ ref('stg_trips') }}
),

payment_analysis AS (
    SELECT
        p.payment_id,
        p.trip_id,
        p.payment_status,
        p.payment_provider,
        p.amount,
        p.fee,
        p.currency, 
        p.created_at,
        
        -- Trip info
        t.trip_status,
        t.actual_fare,
        
        -- Payment date parts
        DATE(p.created_at) AS payment_date,
        EXTRACT(HOUR FROM p.created_at) AS payment_hour,
        EXTRACT(DAYOFWEEK FROM p.created_at) AS payment_day_of_week,
        
        -- Payment success/failure
        CASE WHEN p.payment_status = 'success' THEN 1 ELSE 0 END AS is_successful,
        CASE WHEN p.payment_status = 'failed' THEN 1 ELSE 0 END AS is_failed,
        
        -- Fee percentage
        CASE
            WHEN p.amount > 0 AND p.fee IS NOT NULL
                THEN (p.fee / p.amount) * 100
            ELSE NULL
        END AS fee_percentage,
        
        -- Payment vs fare comparison
        CASE
            WHEN t.actual_fare IS NOT NULL AND p.amount IS NOT NULL
                THEN p.amount - t.actual_fare
            ELSE NULL
        END AS amount_difference
        
    FROM payments p
    LEFT JOIN trips t ON p.trip_id = t.trip_id
),

-- Payment failure rate by provider
provider_stats AS (
    SELECT
        payment_provider,
        COUNT(*) AS total_payments,
        SUM(is_successful) AS successful_payments,
        SUM(is_failed) AS failed_payments,
        ROUND(SAFE_DIVIDE(SUM(is_failed) * 100, COUNT(*)), 2) AS failure_rate_percentage
    FROM payment_analysis
    GROUP BY payment_provider
)

SELECT 
    pa.*,
    ps.failure_rate_percentage AS provider_failure_rate
FROM payment_analysis pa
LEFT JOIN provider_stats ps ON pa.payment_provider = ps.payment_provider