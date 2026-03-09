{{
    config(
        materialized='view'
        )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'cities_raw') }}
    WHERE city_id IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY city_id
            ORDER BY launch_date DESC  
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        CAST(city_id AS INT) AS city_id,
        CASE
            WHEN city_name IS NULL OR TRIM(city_name) = '' THEN 'unknown'
        END AS city_name,
        CASE
            WHEN country IS NULL OR TRIM(country) = '' THEN 'unknown'
            ELSE UPPER(TRIM(country)) 
        END AS country_code,
        CAST(launch_date AS DATE) AS launch_date,
    
        
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned
WHERE city_id IS NOT NULL