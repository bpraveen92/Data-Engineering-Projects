{% macro safe_divide(numerator, denominator, fallback=0) %}
    iff({{ denominator }} = 0 or {{ denominator }} is null,
        {{ fallback }},
        {{ numerator }} / {{ denominator }}
    )
{% endmacro %}
