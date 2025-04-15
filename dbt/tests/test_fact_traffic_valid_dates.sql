-- Test that dates are valid and not in the future
SELECT *
FROM {{ ref('fact_traffic') }}
WHERE toll_date > CURRENT_DATE() 