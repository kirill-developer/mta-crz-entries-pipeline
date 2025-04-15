-- Test that peak hours are between 0 and 23
SELECT *
FROM {{ ref('fact_traffic') }}
WHERE hour < 0 OR hour > 23 