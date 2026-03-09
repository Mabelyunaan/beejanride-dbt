{{
    config(
        materialized='table',
        tags=['intermediate', 'trips', 'core']
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

-- Find duplicate payments
duplicate_payments AS (
    SELECT 
        trip_id,
        COUNT(*) as payment_attempts,
        COUNT(DISTINCT payment_id) as unique_payments,
        CASE 
            WHEN COUNT(*) > 1 THEN TRUE 
            ELSE FALSE 
        END as has_duplicate_payments
    FROM payments
    GROUP BY trip_id
),

-- Trip details with all required metrics
trip_details AS (
    SELECT
        -- IDs
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.city_id,
        t.vehicle_id,
        
        -- Timestamps
        t.requested_at,
        t.pickup_at,
        t.dropoff_at,
        
        -- TRIP DURATION MINUTES
        CASE
            WHEN t.trip_status = 'completed' 
                 AND t.pickup_at IS NOT NULL 
                 AND t.dropoff_at IS NOT NULL
                THEN TIMESTAMP_DIFF(t.dropoff_at, t.pickup_at, MINUTE)
            ELSE NULL
        END AS trip_duration_minutes,
        
        -- Fare information
        t.estimated_fare,
        t.actual_fare,
        t.surge_multiplier,
        
        -- CORPORATE TRIP FLAG
        CASE
            WHEN t.is_corporate = TRUE THEN 'Corporate'
            ELSE 'Personal'
        END AS trip_type,
        t.is_corporate AS is_corporate_trip,
        
        -- Payment information
        p.payment_id,
        p.payment_status,
        p.payment_provider,
        p.amount AS payment_amount,
        p.fee AS processing_fee,
        p.currency,  
        
        --  NET REVENUE CALCULATION
        COALESCE(t.actual_fare, 0) - COALESCE(p.fee, 0) AS net_revenue,
        
        -- Duplicate payment info
        COALESCE(dp.has_duplicate_payments, FALSE) AS has_duplicate_payments,
        COALESCE(dp.payment_attempts, 0) AS payment_attempts,
        
        --FRAUD INDICATORS
        
        -- 1. DUPLICATE TRIP PAY
        COALESCE(dp.has_duplicate_payments, FALSE) AS fraud_duplicate_payments,
        
        -- 2. FAILED PAYMENT ON COMPLETED TRIP
        CASE
            WHEN t.trip_status = 'completed' 
                 AND p.payment_status = 'failed'
                THEN TRUE
            ELSE FALSE
        END AS fraud_failed_payment,
        
        -- 3. EXTREME SURGE MULTIPLIER (>10)
        CASE
            WHEN t.surge_multiplier > 10 THEN TRUE
            ELSE FALSE
        END AS fraud_extreme_surge,
        
        -- 4. Trip completed but no payment
        CASE
            WHEN t.trip_status = 'completed' AND p.payment_id IS NULL
                THEN TRUE
            ELSE FALSE
        END AS fraud_no_payment,
        
        -- 5. Payment amount mismatch
        CASE
            WHEN t.trip_status = 'completed' 
                 AND p.amount IS NOT NULL 
                 AND t.actual_fare IS NOT NULL
                 AND ABS(p.amount - t.actual_fare) > 1
                THEN TRUE
            ELSE FALSE
        END AS fraud_amount_mismatch,
        
        -- OVERALL FRAUD FLAG
        CASE
            WHEN COALESCE(dp.has_duplicate_payments, FALSE) = TRUE
                 OR (t.trip_status = 'completed' AND p.payment_status = 'failed')
                 OR t.surge_multiplier > 10
                 OR (t.trip_status = 'completed' AND p.payment_id IS NULL)
                 OR (t.trip_status = 'completed' 
                     AND p.amount IS NOT NULL 
                     AND t.actual_fare IS NOT NULL
                     AND ABS(p.amount - t.actual_fare) > 1)
                THEN TRUE
            ELSE FALSE
        END AS is_fraud_flag,
        
        -- Surge impact categories
        CASE 
            WHEN t.surge_multiplier <= 1.0 THEN 'No Surge'
            WHEN t.surge_multiplier <= 1.5 THEN 'Low Surge'
            WHEN t.surge_multiplier <= 2.0 THEN 'Medium Surge'
            WHEN t.surge_multiplier <= 3.0 THEN 'High Surge'
            ELSE 'Extreme Surge'
        END AS surge_impact_category,
        
        -- Gross vs Net
        t.actual_fare AS gross_revenue,
        COALESCE(t.actual_fare, 0) - COALESCE(p.fee, 0) AS net_revenue_amount,
        
        -- Metadata
        t.created_at,
        t.updated_at
        
    FROM trips t
    LEFT JOIN payments p ON t.trip_id = p.trip_id
    LEFT JOIN duplicate_payments dp ON t.trip_id = dp.trip_id
    WHERE t.trip_status IN ('completed', 'cancelled', 'no_show')
)

SELECT * FROM trip_details