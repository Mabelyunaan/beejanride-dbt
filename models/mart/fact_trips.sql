{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='trip_sk',
        tags=['fact', 'trips', 'core'],
        schema='core',
        partition_by={
            'field': 'trip_date',
            'data_type': 'date'
        },
        cluster_by=['city_sk', 'driver_sk']
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('int_trip_details') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),

dim_drivers AS (
    SELECT driver_sk, driver_id 
    FROM {{ ref('dim_drivers') }}
    WHERE is_current
),

dim_riders AS (
    SELECT rider_sk, rider_id 
    FROM {{ ref('dim_riders') }}
    WHERE is_current
),

dim_cities AS (
    SELECT city_sk, city_id 
    FROM {{ ref('dim_cities') }}
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['t.trip_id']) }} AS trip_sk,
    
    -- Foreign keys
    d.driver_sk,
    r.rider_sk,
    c.city_sk,
    
    -- Date dimension
    DATE(t.requested_at) AS trip_date,
    
    -- Facts (additive)
    1 AS trip_count,
    t.actual_fare AS gross_revenue,
    t.net_revenue,
    t.surge_multiplier,
    t.trip_duration_minutes,
    
    -- Business flags
    t.trip_type,  -- This is 'Corporate' or 'Personal'
    -- ✅ FIX: Use trip_type instead of trip_status, or add trip_status if needed
    t.is_fraud_flag,
    
    -- Surge impact
    t.surge_impact_category,
    
    -- Timestamps
    t.requested_at,
    t.pickup_at,
    t.dropoff_at,
    
    -- Metadata
    t.created_at,
    CURRENT_TIMESTAMP() AS inserted_at
    
FROM trips t
LEFT JOIN dim_drivers d ON t.driver_id = d.driver_id
LEFT JOIN dim_riders r ON t.rider_id = r.rider_id
LEFT JOIN dim_cities c ON t.city_id = c.city_id