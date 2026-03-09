{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'payments_raw') }}
    WHERE payment_id IS NOT NULL 
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY created_at DESC 
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        CAST(payment_id AS INT) AS payment_id,
        CAST(trip_id AS INT) AS trip_id,
        
        CASE
            WHEN LOWER(TRIM(payment_status)) IN ('success')
                THEN 'success'
            WHEN LOWER(TRIM(payment_status)) IN ('failed')
                THEN 'failed'
        END AS payment_status,
        CASE
            WHEN LOWER(TRIM(payment_provider)) IN ('stripe')
                THEN 'stripe'
            WHEN LOWER(TRIM(payment_provider)) IN ('paypal')
                THEN 'paypal'
        END AS payment_provider,
        CASE
            WHEN CAST(amount AS NUMERIC) < 0 THEN 0 
            ELSE CAST(amount AS NUMERIC)
        END AS amount,
        CASE
            WHEN CAST(fee AS NUMERIC) < 0 THEN 0
            WHEN CAST(fee AS NUMERIC) IS NULL THEN 0 
            ELSE CAST(fee AS NUMERIC)
        END AS fee,
        
        CASE
            WHEN currency IS NULL OR TRIM(currency) = '' THEN 'UNKNOWN'
            ELSE UPPER(TRIM(currency))
        END AS currency,
        CAST(created_at AS TIMESTAMP) AS created_at,
        
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned
WHERE payment_id IS NOT NULL
  AND trip_id IS NOT NULL  