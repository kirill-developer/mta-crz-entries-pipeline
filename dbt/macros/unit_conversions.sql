{% macro to_miles(meters) %}
  {{ return(meters/1609.34) }}
{% endmacro %}

{% macro to_dollars(cents) %}
  {{ return(cents/100) }}
{% endmacro %}