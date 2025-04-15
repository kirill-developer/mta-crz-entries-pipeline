-- Test that entries are non-negative
SELECT *
FROM {{ ref('fact_traffic') }}
WHERE entries < 0
    OR excluded_entries < 0 