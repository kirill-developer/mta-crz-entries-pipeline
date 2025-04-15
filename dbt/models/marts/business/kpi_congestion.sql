{{
  config(
    materialized='table'
  )
}}

SELECT
  toll_date,
  detection_region,
  borough,
  SUM(entries) AS total_entries,
  SUM(excluded_entries) AS total_excluded,
  ROUND(SUM(entries) / NULLIF(SUM(excluded_entries), 0), 2) AS entry_ratio,
  {{ congestion_severity('SUM(entries)') }} AS severity_level
FROM
  {{ ref('fact_traffic') }}
GROUP BY 1, 2, 3