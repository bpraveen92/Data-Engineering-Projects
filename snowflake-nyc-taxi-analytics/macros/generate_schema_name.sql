-- dev target → dev_<schema>; prod target → <schema>; no custom schema → default
{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- if target.name == 'dev' and custom_schema_name is not none -%}
    dev_{{ custom_schema_name | trim }}

  {%- elif custom_schema_name is not none -%}
    {{ custom_schema_name | trim }}

  {%- else -%}
    {{ default_schema }}

  {%- endif -%}

{%- endmacro %}
