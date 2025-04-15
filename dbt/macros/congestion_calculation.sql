{% macro congestion_severity(entries) %}
  CASE
    WHEN {{ entries }} > 10000 THEN 'SEVERE'
    WHEN {{ entries }} > 5000 THEN 'HIGH'
    WHEN {{ entries }} > 1000 THEN 'MODERATE'
    ELSE 'LOW'
  END
{% endmacro %}