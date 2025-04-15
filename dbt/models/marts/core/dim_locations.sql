{{
  config(
    materialized='table',
    unique_key='detection_region'
  )
}}

WITH zone_mapping AS (
  SELECT DISTINCT
    detection_region,
    CASE
      WHEN detection_region LIKE '%Manhattan%' THEN 'Manhattan'
      WHEN detection_region LIKE '%Bronx%' THEN 'Bronx'
      WHEN detection_region LIKE '%Brooklyn%' THEN 'Brooklyn'
      WHEN detection_region LIKE '%Queens%' THEN 'Queens'
      WHEN detection_region LIKE '%Staten%' THEN 'Staten Island'
      ELSE detection_region
    END AS borough,
    CASE
      WHEN detection_region LIKE '%Bridge%' THEN 'Bridge'
      WHEN detection_region LIKE '%Tunnel%' THEN 'Tunnel'
      WHEN detection_region LIKE '%CBD%' THEN 'Central Business District'
      WHEN detection_region LIKE '%Midtown%' THEN 'Midtown'
      ELSE 'Other'
    END AS facility_type,
    detection_region as full_name
  FROM
    {{ ref('stg_nyc_crz_entries') }}
)

SELECT
  detection_region,
  borough,
  facility_type,
  full_name
FROM
  zone_mapping