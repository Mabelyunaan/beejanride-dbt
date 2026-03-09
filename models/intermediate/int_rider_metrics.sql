{{
    config(
        materialized='table'
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
    WHERE trip_status = 'completed'
),

riders AS (
    SELECT * FROM {{ ref('stg_riders') }}
),

-- Rider lifetime value
rider_trip_stats AS (
    SELECT
        rider_id,
        COUNT(*) AS lifetime_trips,
        SUM(actual_fare) AS lifetime_revenue,
        AVG(actual_fare) AS avg_trip_value,
        MIN(requested_at) AS first_trip_date,
        MAX(requested_at) AS last_trip_date,
        
        -- Time between first and last trip
        TIMESTAMP_DIFF(
            MAX(requested_at), 
            MIN(requested_at), 
            DAY
        ) AS customer_lifetime_days,
        
        -- Average trips per month
        SAFE_DIVIDE(
            COUNT(*) * 30,
            TIMESTAMP_DIFF(MAX(requested_at), MIN(requested_at), DAY) + 1
        ) AS avg_trips_per_month,
        
        -- Corporate vs personal split
        SUM(CASE WHEN is_corporate = TRUE THEN actual_fare ELSE 0 END) AS corporate_revenue,
        SUM(CASE WHEN is_corporate = FALSE THEN actual_fare ELSE 0 END) AS personal_revenue,
        COUNT(CASE WHEN is_corporate = TRUE THEN 1 END) AS corporate_trips,
        COUNT(CASE WHEN is_corporate = FALSE THEN 1 END) AS personal_trips
        
    FROM trips
    GROUP BY rider_id
),

rider_details AS (
    SELECT
        r.rider_id,
        r.signup_date,
        r.country,
        CASE 
            WHEN UPPER(r.country_code) IN ('UK', 'GB', 'UNITED KINGDOM') THEN 'UK'
            ELSE 'International'
        END AS region,
        r.referral_code,
        
        -- Days since signup
        DATE_DIFF(CURRENT_DATE(), r.signup_date, DAY) AS days_as_rider,
        
        -- Trip stats
        COALESCE(rt.lifetime_trips, 0) AS lifetime_trips,
        COALESCE(rt.lifetime_revenue, 0) AS rider_ltv,
        COALESCE(rt.avg_trip_value, 0) AS avg_trip_value,
        rt.first_trip_date,
        rt.last_trip_date,
        
        -- LTV segments
        CASE
            WHEN COALESCE(rt.lifetime_revenue, 0) >= 1000 THEN 'High Value'
            WHEN COALESCE(rt.lifetime_revenue, 0) >= 500 THEN 'Medium Value'
            WHEN COALESCE(rt.lifetime_revenue, 0) >= 100 THEN 'Low Value'
            WHEN rt.first_trip_date IS NULL THEN 'Signed Up - No Trips'
            ELSE 'New'
        END AS ltv_segment,
        
        -- Rider status
        CASE
            WHEN rt.first_trip_date IS NULL THEN 'Inactive'
            WHEN rt.last_trip_date < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY) THEN 'Churned'
            WHEN rt.last_trip_date < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN 'At Risk'
            ELSE 'Active'
        END AS rider_status,
        
        -- Corporate vs personal split
        COALESCE(rt.corporate_trips, 0) AS corporate_trips,
        COALESCE(rt.personal_trips, 0) AS personal_trips,
        COALESCE(rt.corporate_revenue, 0) AS corporate_revenue,
        COALESCE(rt.personal_revenue, 0) AS personal_revenue,
        
        -- Referral effectiveness
        CASE
            WHEN r.referral_code IS NOT NULL AND r.referral_code != '' AND rt.lifetime_trips > 0 THEN 'Successful Referral'
            WHEN r.referral_code IS NOT NULL AND r.referral_code != '' THEN 'Unused Referral'
            ELSE 'No Referral'
        END AS referral_effectiveness
        
    FROM riders r
    LEFT JOIN rider_trip_stats rt ON r.rider_id = rt.rider_id
)

SELECT * FROM rider_details