{% macro classify_time_of_day(hour_expr) %}
    case
        when {{ hour_expr }} between 6  and 9  then 'morning_rush'
        when {{ hour_expr }} between 10 and 15 then 'midday'
        when {{ hour_expr }} between 16 and 19 then 'evening_rush'
        when {{ hour_expr }} between 20 and 22 then 'evening'
        else 'overnight'    -- hours 0–5 and 23
    end
{% endmacro %}
