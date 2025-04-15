{{
  config(
    materialized='table'
  )
}}

WITH date_spine AS (
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT MIN(toll_date) FROM {{ ref('stg_nyc_crz_entries') }}),
    (SELECT MAX(toll_date) FROM {{ ref('stg_nyc_crz_entries') }}),
    INTERVAL 1 DAY
  )) AS date
)

SELECT
  date as toll_date,
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week_num,
  FORMAT_DATE('%A', date) AS day_of_week_name,
  EXTRACT(MONTH FROM date) AS month,
  EXTRACT(QUARTER FROM date) AS quarter,
  EXTRACT(YEAR FROM date) AS year
FROM date_spine