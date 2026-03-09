{{
    config(
        materialized='table'
    )
}}

WITH driver_performance AS (
    SELECT
        d.driver_id,
        d.driver_status,
        d.driver_tier,
        d.rating_category,
        c.city_name,
        
        -- Trip metrics
        COUNT(DISTINCT t.trip_id) AS trips_completed,
        SUM(t.gross_revenue) AS total_revenue,
        AVG(t.trip_duration_minutes) AS avg_trip_time,
        
        -- Rankings
        RANK() OVER (PARTITION BY c.city_name ORDER BY SUM(t.gross_revenue) DESC) AS city_rank,
        RANK() OVER (ORDER BY SUM(t.gross_revenue) DESC) AS overall_rank
        
    FROM {{ ref('dim_drivers') }} d
    JOIN {{ ref('fact_trips') }} t ON d.driver_sk = t.driver_sk
    JOIN {{ ref('dim_cities') }} c ON t.city_sk = c.city_sk
    WHERE t.trip_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY 1, 2, 3, 4, 5
)

SELECT * FROM driver_performance
WHERE city_rank <= 10
ORDER BY city_name, city_rank