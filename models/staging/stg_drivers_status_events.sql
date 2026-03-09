{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'driver_status_events_raw') }} 
    WHERE event_id IS NOT NULL
      AND driver_id IS NOT NULL 
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY event_timestamp DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        CAST(event_id AS INT64) AS event_id,
        CAST(driver_id AS INT64) AS driver_id,
        
        CASE
            WHEN LOWER(TRIM(status)) IN ('online')
                THEN 'online'
            WHEN LOWER(TRIM(status)) IN ('offline')
                THEN 'offline'
            ELSE 'unknown' 
        END AS driver_status,
        
        CAST(event_timestamp AS TIMESTAMP) AS event_timestamp
        
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned
WHERE event_id IS NOT NULL
  AND driver_id IS NOT NULL
  AND driver_status != 'unknown'