{{
    config(
        materialized='table'
    )
}}

WITH daily_revenue AS (
    SELECT
        DATE(t.requested_at) AS revenue_date,
        c.city_name,
        c.country_code,
        
        -- Revenue metrics
        COUNT(DISTINCT t.trip_id) AS total_trips,
        SUM(t.gross_revenue) AS gross_revenue,
        SUM(t.net_revenue) AS net_revenue,
        
        -- Corporate vs personal split
        SUM(CASE WHEN t.trip_type = 'Corporate' THEN t.gross_revenue ELSE 0 END) AS corporate_revenue,
        SUM(CASE WHEN t.trip_type = 'Personal' THEN t.gross_revenue ELSE 0 END) AS personal_revenue,
        
        -- Fraud metrics
        SUM(CASE WHEN t.is_fraud_flag THEN 1 ELSE 0 END) AS fraud_trips,
        
        -- Average metrics
        AVG(t.trip_duration_minutes) AS avg_trip_duration,
        AVG(t.gross_revenue) AS avg_ticket_size
        
    FROM {{ ref('fact_trips') }} t
    JOIN {{ ref('dim_cities') }} c ON t.city_sk = c.city_sk
    GROUP BY 1, 2, 3
)

SELECT * FROM daily_revenue
ORDER BY revenue_date DESC, city_name