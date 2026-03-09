  {{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'trips_raw') }} 
    WHERE trip_id IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id
            ORDER BY updated_at DESC
        ) AS row_num
    FROM source
),

casted AS (
    SELECT
        CAST(trip_id AS INT64) AS trip_id,
        CAST(rider_id AS INT64) AS rider_id,
        CAST(driver_id AS INT64) AS driver_id,
        vehicle_id,
        CAST(city_id AS INT64) AS city_id,
        CAST(requested_at AS TIMESTAMP) AS requested_at,
        CAST(pickup_at AS TIMESTAMP) AS pickup_at,
        CAST(dropoff_at AS TIMESTAMP) AS dropoff_at,
        LOWER(TRIM(status)) AS trip_status,
        CAST(estimated_fare AS NUMERIC) AS estimated_fare,
        CAST(actual_fare AS NUMERIC) AS actual_fare,
        CAST(surge_multiplier AS NUMERIC) AS surge_multiplier,
        LOWER(TRIM(payment_method)) AS payment_method,
        COALESCE(CAST(is_corporate AS BOOLEAN), FALSE) AS is_corporate,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at 
        
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * 
FROM casted
WHERE trip_id IS NOT NULL
  AND rider_id IS NOT NULL
  AND driver_id IS NOT NULL