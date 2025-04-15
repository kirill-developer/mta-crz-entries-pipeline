-- Test for valid vehicle classes
WITH valid_classes AS (
    SELECT DISTINCT vehicle_class
    FROM {{ ref('fact_traffic') }}
)
SELECT *
FROM valid_classes
WHERE vehicle_class NOT IN (
    'Cars, Pickups and Vans',
    'TLC Taxi/FHV',
    'Single-Unit Trucks',
    'Buses',
    'Motorcycles',
    'Multi-Unit Trucks') 