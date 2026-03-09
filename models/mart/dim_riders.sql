{{
    config(
        materialized='table'
    )
}}

WITH riders AS (
    SELECT * FROM {{ ref('int_rider_metrics') }}
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['rider_id']) }} AS rider_sk,
    
    -- Natural key
    rider_id,
    
    -- Attributes
    signup_date,
    country_code,
    region,
    ltv_segment,
    rider_status,
    
    -- Metrics
    lifetime_trips,
    rider_ltv,
    avg_trip_value,
    corporate_trips,
    personal_trips,
    corporate_revenue,
    personal_revenue,
    
    -- Date dimensions
    EXTRACT(YEAR FROM signup_date) AS signup_year,
    EXTRACT(MONTH FROM signup_date) AS signup_month,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
    
FROM riders