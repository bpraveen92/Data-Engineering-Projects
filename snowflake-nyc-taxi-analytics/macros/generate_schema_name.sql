/*
  generate_schema_name
  --------------------
  Custom macro that overrides dbt's default schema naming behaviour.

  Why this macro exists
  ---------------------
  By default, dbt writes all models to the single schema specified in profiles.yml.
  But our dbt_project.yml assigns a custom schema per model group:

    staging    → schema: staging
    marts/core → schema: marts_core
    marts/fin  → schema: marts_finance
    seeds      → schema: raw

  Without this macro, dbt would write to `<default_schema>_staging`,
  `<default_schema>_marts_core` etc. — prepending the profile schema name.

  With this macro:
    dev  target → DEV_STAGING, DEV_MARTS_CORE, DEV_MARTS_FINANCE, RAW
    prod target → STAGING, MARTS_CORE, MARTS_FINANCE, RAW

  The dev_ prefix prevents dev runs from overwriting production schemas.
  This mirrors how Databricks' `mode: development` adds a [dev username] prefix
  to prevent collisions in a shared catalog.

  How dbt calls this macro
  ------------------------
  dbt calls generate_schema_name(custom_schema_name, node) for every model
  before it creates the target relation.  The return value becomes the schema
  portion of the fully-qualified object name:
    <database>.<schema>.<model_name>

  Arguments
  ---------
  custom_schema_name : string | none
    The value from +schema in dbt_project.yml (e.g. 'marts_core').
    None when the model has no custom schema set.
  node : dict
    The dbt graph node for the model (unused here, but available if needed).
*/
{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- if target.name == 'dev' and custom_schema_name is not none -%}
    {# Dev target: prefix every custom schema with "dev_" #}
    dev_{{ custom_schema_name | trim }}

  {%- elif custom_schema_name is not none -%}
    {# Prod target (or any non-dev target): use the schema name as-is #}
    {{ custom_schema_name | trim }}

  {%- else -%}
    {# No custom schema defined: fall back to the default schema in profiles.yml #}
    {{ default_schema }}

  {%- endif -%}

{%- endmacro %}
