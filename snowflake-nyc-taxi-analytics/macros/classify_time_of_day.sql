/*
  classify_time_of_day
  --------------------
  Classifies an integer hour (0–23) into a named time-of-day bucket.

  Time buckets are modelled on NYC commuter patterns — this produces a
  dimension column that is immediately useful for demand analysis without
  requiring the downstream analyst to remember the hour ranges.

  Buckets:
    morning_rush   06–09  (peak inbound commute)
    midday         10–15  (lunch + midday travel)
    evening_rush   16–19  (peak outbound commute)
    evening        20–22  (dinners, entertainment)
    overnight      23–05  (late night / early morning)

  Usage
  -----
  -- In a model SQL file:
  {{ classify_time_of_day('hour(pickup_datetime)') }} as time_of_day

  Arguments
  ---------
  hour_expr : SQL expression that evaluates to an integer 0–23
*/
{% macro classify_time_of_day(hour_expr) %}
    case
        when {{ hour_expr }} between 6  and 9  then 'morning_rush'
        when {{ hour_expr }} between 10 and 15 then 'midday'
        when {{ hour_expr }} between 16 and 19 then 'evening_rush'
        when {{ hour_expr }} between 20 and 22 then 'evening'
        else 'overnight'    -- hours 0–5 and 23
    end
{% endmacro %}
