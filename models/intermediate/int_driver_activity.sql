{{
    config(
        materialized='table'
    )
}}

WITH status_events AS (
    SELECT * FROM {{ ref('stg_drivers_status_events') }}
),

-- Calculate online sessions
online_sessions AS (
    SELECT
        driver_id,
        event_timestamp AS session_start,
        LEAD(event_timestamp) OVER (
            PARTITION BY driver_id 
            ORDER BY event_timestamp
        ) AS session_end,
        LEAD(driver_status) OVER (
            PARTITION BY driver_id 
            ORDER BY event_timestamp
        ) AS next_status
    FROM status_events
    WHERE driver_status = 'online'
),

session_durations AS (
    SELECT
        driver_id,
        session_start,
        CASE 
            WHEN next_status = 'offline' THEN session_end
            ELSE NULL
        END AS session_end,
        CASE 
            WHEN next_status = 'offline' 
                THEN TIMESTAMP_DIFF(session_end, session_start, MINUTE)
            ELSE NULL
        END AS session_duration_minutes,
        DATE(session_start) AS activity_date
    FROM online_sessions
),

-- Daily driver activity summary
daily_driver_activity AS (
    SELECT
        driver_id,
        activity_date,
        COUNT(*) AS online_sessions,
        AVG(session_duration_minutes) AS avg_session_duration,
        SUM(session_duration_minutes) AS total_online_minutes,
        MIN(session_start) AS first_online,
        MAX(session_start) AS last_online
    FROM session_durations
    WHERE session_duration_minutes IS NOT NULL
    GROUP BY driver_id, activity_date
)

SELECT * FROM daily_driver_activity