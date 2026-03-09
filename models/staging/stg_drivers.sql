  {{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'drivers_raw') }}
    WHERE driver_id IS NOT NULL  
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY driver_id
            ORDER BY updated_at DESC 
        ) AS row_num
    FROM source
),
cleaned AS (
    SELECT
        CAST(driver_id AS INT64) AS driver_id,
        CAST(city_id AS INT64) AS city_id,
        CAST(vehicle_id AS INT64) AS vehicle_id,
        CAST(onboarding_date AS DATE) AS onboarding_date,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        LOWER(TRIM(driver_status)) AS driver_status,
        
        CASE
            WHEN CAST(rating AS FLOAT64) BETWEEN 1 AND 5 
                THEN CAST(rating AS FLOAT64)
            WHEN CAST(rating AS FLOAT64) > 5 THEN 5.0  
            WHEN CAST(rating AS FLOAT64) < 1 THEN 1.0 
            ELSE NULL  
        END AS rating

    FROM deduplicated
    WHERE row_num = 1  
)

SELECT * FROM cleaned
WHERE driver_id IS NOT NULL
  AND city_id IS NOT NULL