{% macro categorize_vehicle(vehicle_class) %}
  CASE
    WHEN {{ vehicle_class }} = '1' THEN 'Cars, Pickups and Vans'
    WHEN {{ vehicle_class }} = '2' THEN 'Single-Unit Trucks'
    WHEN {{ vehicle_class }} = '3' THEN 'Multi-Unit Trucks'
    WHEN {{ vehicle_class }} = '4' THEN 'Buses'
    WHEN {{ vehicle_class }} = '5' THEN 'Motorcycles'
    WHEN {{ vehicle_class }} = 'TLC' THEN 'TLC Taxi/FHV'
    ELSE 'Other'
  END
{% endmacro %}

{% macro get_borough(region) %}
  CASE
    WHEN {{ region }} LIKE '%Manhattan%' THEN 'Manhattan'
    WHEN {{ region }} LIKE '%Brooklyn%' THEN 'Brooklyn'
    ELSE 'Other Boroughs'
  END
{% endmacro %}