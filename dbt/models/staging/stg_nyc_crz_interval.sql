{{
  config(
    schema='nyc_traffic',
    materialized='view'
  )
}}

SELECT
  toll_date,
  vehicle_class,
  detection_group,
  detection_region,
  crz_entries,
  excluded_roadway_entries,
  EXTRACT(HOUR FROM toll_date) AS hour
FROM {{ source('raw', 'congestion_pricing_entries') }} 