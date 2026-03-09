{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'riders_raw') }}
    WHERE rider_id IS NOT NULL
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY rider_id
            ORDER BY created_at DESC 
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        CAST(rider_id AS INT) AS rider_id,
        CAST(signup_date AS DATE) AS signup_date,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CASE
            WHEN country IS NULL OR TRIM(country) = '' THEN 'unknown'
            ELSE UPPER(TRIM(country))
        END AS country_code,
        TRIM(referral_code) AS referral_code,   
    
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned
WHERE rider_id IS NOT NULL
  AND signup_date IS NOT NULL 