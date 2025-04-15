{{
  config(
    materialized='table',
    unique_key=['toll_date', 'hour', 'vehicle_class', 'detection_region']
  )
}}

SELECT
    s.toll_date,
    s.hour,
    s.vehicle_class,
    s.detection_group,
    s.detection_region,
    s.crz_entries AS entries,
    s.excluded_roadway_entries AS excluded_entries,
    t.day_of_week_name,
    t.month,
    l.borough,
    l.full_name AS congestion_zone
FROM
    {{ ref('stg_nyc_crz_entries') }} s
JOIN
    {{ ref('dim_time') }} t ON s.toll_date = t.toll_date
JOIN
    {{ ref('dim_locations') }} l ON s.detection_region = l.detection_region