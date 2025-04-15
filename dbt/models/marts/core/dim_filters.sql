{{
  config(
    materialized='table',
    schema='nyc_traffic'
  )
}}

WITH vehicle_classes AS (
    SELECT DISTINCT vehicle_class
    FROM {{ ref('stg_nyc_crz_entries') }}
),

locations AS (
    SELECT DISTINCT 
        borough,
        full_name as location_name,
        detection_region
    FROM {{ ref('dim_locations') }}
),

time_periods AS (
    SELECT DISTINCT
        month,
        day_of_week_name
    FROM {{ ref('dim_time') }}
),

hours AS (
    SELECT DISTINCT hour
    FROM {{ ref('stg_nyc_crz_entries') }}
)

SELECT
    'vehicle_class' as filter_type,
    vehicle_class as filter_value,
    vehicle_class as filter_display
FROM vehicle_classes

UNION ALL

SELECT
    'borough' as filter_type,
    borough as filter_value,
    borough as filter_display
FROM locations
WHERE borough IS NOT NULL

UNION ALL

SELECT
    'location' as filter_type,
    detection_region as filter_value,
    location_name as filter_display
FROM locations

UNION ALL

SELECT
    'month' as filter_type,
    CAST(month AS STRING) as filter_value,
    CAST(month AS STRING) as filter_display
FROM time_periods

UNION ALL

SELECT
    'day_of_week' as filter_type,
    day_of_week_name as filter_value,
    day_of_week_name as filter_display
FROM time_periods

UNION ALL

SELECT
    'hour' as filter_type,
    CAST(hour AS STRING) as filter_value,
    CAST(hour AS STRING) as filter_display
FROM hours 