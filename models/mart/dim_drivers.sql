{{
    config(
        materialized='table'
    )
}}

WITH drivers AS (
    SELECT * FROM {{ ref('init_driver_metrics') }}
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['driver_id']) }} AS driver_sk,
    
    -- Natural key
    driver_id,
    
    -- Attributes
    driver_status,
    city_id,
    rating,
    onboarding_date,
    driver_tier,
    rating_category,
    driver_health_status,
    
    -- Metrics (slowly changing)
    lifetime_trips,
    lifetime_revenue,
    avg_trip_fare,
    days_active,
    
    -- Date dimensions
    EXTRACT(YEAR FROM onboarding_date) AS onboarding_year,
    EXTRACT(MONTH FROM onboarding_date) AS onboarding_month,
    
    -- Metadata for SCD
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
    
FROM drivers