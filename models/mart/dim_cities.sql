{{
    config(
        materialized='table'
    )
}}

WITH cities AS (
    SELECT * FROM {{ ref('stg_cities') }}
)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['city_id']) }} AS city_sk,
    
    -- Natural key
    city_id,
    
    -- Attributes
    city_name,
    country_code,
    launch_date,
    
    -- Derived
    CASE 
        WHEN country_code IN ('UK', 'GB') THEN 'Domestic'
        ELSE 'International'
    END AS market_type,
    
    EXTRACT(YEAR FROM launch_date) AS launch_year,
    EXTRACT(MONTH FROM launch_date) AS launch_month,
    
    -- City age
    DATE_DIFF(CURRENT_DATE(), launch_date, DAY) AS days_operating,
    DATE_DIFF(CURRENT_DATE(), launch_date, MONTH) AS months_operating
    
FROM cities