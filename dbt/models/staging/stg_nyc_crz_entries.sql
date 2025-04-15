{{
  config(
    schema='nyc_traffic',
    materialized='view'
  )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'congestion_pricing_entries') }}
),

transformed AS (
    SELECT
        DATE(toll_date) as toll_date,
        EXTRACT(HOUR FROM toll_hour) as hour,
        REGEXP_REPLACE(vehicle_class, r'^[0-9]+ - ', '') as vehicle_class,
        detection_group,
        detection_region,
        crz_entries,
        excluded_roadway_entries
    FROM source
)

SELECT *
FROM transformed