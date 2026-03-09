{{
    config(
        materialized='table'
    )
}}

WITH trips AS (
    SELECT * FROM {{ ref('stg_trips') }}
    WHERE trip_status = 'completed'
),

drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

driver_status_events AS (
    SELECT * FROM {{ ref('stg_drivers_status_events') }}
),

-- Driver lifetime trips
driver_trip_stats AS (
    SELECT
        driver_id,
        COUNT(*) AS lifetime_trips,
        SUM(actual_fare) AS lifetime_revenue,
        AVG(actual_fare) AS avg_trip_fare,
        MIN(requested_at) AS first_trip_date,
        MAX(requested_at) AS last_trip_date,
        COUNT(DISTINCT DATE(requested_at)) AS days_active,
        
        -- Average trips per day
        SAFE_DIVIDE(
            COUNT(*),
            TIMESTAMP_DIFF(MAX(requested_at), MIN(requested_at), DAY) + 1
        ) AS avg_trips_per_active_day
        
    FROM trips
    GROUP BY driver_id
),

-- Driver activity from status events
driver_activity AS (
    SELECT
        driver_id,
        COUNT(*) AS total_status_changes,
        SUM(CASE WHEN driver_status = 'online' THEN 1 ELSE 0 END) AS online_events,
        SUM(CASE WHEN driver_status = 'offline' THEN 1 ELSE 0 END) AS offline_events,
        MIN(event_timestamp) AS first_event,
        MAX(event_timestamp) AS last_event
    FROM driver_status_events
    GROUP BY driver_id
),

driver_details AS (
    SELECT
        d.driver_id,
        d.driver_status,
        d.city_id,
        d.rating,
        d.onboarding_date,
        
        -- Driver tenure
        DATE_DIFF(CURRENT_DATE(), d.onboarding_date, DAY) AS days_as_driver,
        DATE_DIFF(CURRENT_DATE(), d.onboarding_date, MONTH) AS months_as_driver,
        
        -- Trip metrics
        COALESCE(dts.lifetime_trips, 0) AS lifetime_trips,
        COALESCE(dts.lifetime_revenue, 0) AS lifetime_revenue,
        COALESCE(dts.avg_trip_fare, 0) AS avg_trip_fare,
        dts.first_trip_date,
        dts.last_trip_date,
        COALESCE(dts.days_active, 0) AS days_active,
        
        -- Activity metrics
        da.total_status_changes,
        da.online_events,
        da.offline_events,
        da.first_event,
        da.last_event,
        
        -- Driver churn tracking
        CASE
            WHEN d.driver_status = 'inactive' THEN 'Churned'
            WHEN d.driver_status = 'suspended' THEN 'Suspended'
            WHEN dts.last_trip_date IS NULL THEN 'Never Active'
            -- Compare timestamp with timestamp
            WHEN dts.last_trip_date < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN 'At Risk'
            WHEN dts.last_trip_date < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) THEN 'Recent'
            ELSE 'Active'
        END AS driver_health_status,
        
        -- Rating categories
        CASE
            WHEN d.rating >= 4.5 THEN 'Top Rated'
            WHEN d.rating >= 4.0 THEN 'Good'
            WHEN d.rating >= 3.0 THEN 'Average'
            WHEN d.rating IS NOT NULL THEN 'Needs Improvement'
            ELSE 'No Ratings'
        END AS rating_category,
        
        -- Top driver potential
        CASE
            WHEN COALESCE(dts.lifetime_trips, 0) >= 1000 THEN 'Platinum'
            WHEN COALESCE(dts.lifetime_trips, 0) >= 500 THEN 'Gold'
            WHEN COALESCE(dts.lifetime_trips, 0) >= 100 THEN 'Silver'
            ELSE 'Bronze'
        END AS driver_tier
        
    FROM drivers d
    LEFT JOIN driver_trip_stats dts ON d.driver_id = dts.driver_id
    LEFT JOIN driver_activity da ON d.driver_id = da.driver_id
)

SELECT * FROM driver_details