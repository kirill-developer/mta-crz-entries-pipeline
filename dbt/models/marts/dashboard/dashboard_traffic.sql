{{
    config(
        materialized='table',
        schema='dashboard'
    )
}}

-- Note: Data available from January 28, 2025 to April 10, 2025
-- Source: MTA Congestion Relief Zone Vehicle Entries
-- Updated weekly

WITH traffic_data AS (
    SELECT
        f.toll_date,
        f.hour,
        f.vehicle_class,
        f.detection_group,
        f.detection_region,
        COALESCE(f.entries, 0) as entries,
        COALESCE(f.excluded_entries, 0) as excluded_entries,
        t.day_of_week_name,
        t.month,
        l.borough,
        l.full_name as location_name
    FROM {{ ref('fact_traffic') }} f
    JOIN {{ ref('dim_time') }} t ON f.toll_date = t.toll_date
    JOIN {{ ref('dim_locations') }} l ON f.detection_region = l.detection_region
),

-- Aggregate metrics for different time periods
daily_metrics AS (
    SELECT
        toll_date,
        vehicle_class,
        detection_region,
        borough,
        location_name,
        SUM(entries) as total_entries,
        SUM(excluded_entries) as total_excluded,
        ROUND(SUM(excluded_entries) * 100.0 / NULLIF(SUM(entries), 0), 2) as exempt_ratio
    FROM traffic_data
    GROUP BY 1, 2, 3, 4, 5
),

hourly_metrics AS (
    SELECT
        toll_date,
        hour,
        vehicle_class,
        detection_region,
        borough,
        location_name,
        SUM(entries) as hourly_entries
    FROM traffic_data
    GROUP BY 1, 2, 3, 4, 5, 6
),

peak_hours AS (
    SELECT
        toll_date,
        vehicle_class,
        detection_region,
        borough,
        location_name,
        hour,
        hourly_entries,
        ROW_NUMBER() OVER (PARTITION BY toll_date ORDER BY hourly_entries DESC) as rn
    FROM hourly_metrics
),

final_metrics AS (
    SELECT
        d.*,
        ph.hour as peak_hour,
        ph.hourly_entries as peak_hour_entries
    FROM daily_metrics d
    LEFT JOIN peak_hours ph ON 
        d.toll_date = ph.toll_date 
        AND d.vehicle_class = ph.vehicle_class 
        AND d.detection_region = ph.detection_region
        AND ph.rn = 1
)

SELECT
    f.*,
    df.filter_type,
    df.filter_value,
    df.filter_display
FROM final_metrics f
CROSS JOIN {{ ref('dim_filters') }} df
WHERE 
    (df.filter_type = 'vehicle_class' AND df.filter_value = f.vehicle_class)
    OR (df.filter_type = 'borough' AND df.filter_value = f.borough)
    OR (df.filter_type = 'location' AND df.filter_value = f.detection_region)
    OR (df.filter_type = 'month' AND df.filter_value = CAST(EXTRACT(MONTH FROM f.toll_date) AS STRING))
    OR (df.filter_type = 'day_of_week' AND EXISTS (
        SELECT 1 
        FROM {{ ref('dim_time') }} t 
        WHERE t.toll_date = f.toll_date 
        AND t.day_of_week_name = df.filter_value
    ))
    OR (df.filter_type = 'hour' AND df.filter_value = CAST(f.peak_hour AS STRING)) 