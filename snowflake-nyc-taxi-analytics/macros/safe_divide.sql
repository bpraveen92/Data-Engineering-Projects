/*
  safe_divide
  -----------
  Divides numerator by denominator, returning a fallback value (default 0)
  when the denominator is zero or null.

  In SQL, dividing by zero raises an error.  In analytics models, zero
  denominators are common (e.g. a zone with 0 trips last month) and should
  produce null or 0 rather than crashing the entire model run.

  Usage examples
  --------------
  -- Average fare per trip for a zone (zone may have had 0 trips):
  {{ safe_divide('sum(fare_amount)', 'count(trip_id)') }}

  -- Percentage of airport trips:
  {{ safe_divide('sum(is_airport_trip::int)', 'count(*)', fallback='null') }}

  Arguments
  ---------
  numerator   : SQL expression for the numerator
  denominator : SQL expression for the denominator
  fallback    : value to return when denominator = 0 (default: 0)
*/
{% macro safe_divide(numerator, denominator, fallback=0) %}
    iff({{ denominator }} = 0 or {{ denominator }} is null,
        {{ fallback }},
        {{ numerator }} / {{ denominator }}
    )
{% endmacro %}
