/*
  dim_dates
  ---------
  Date dimension table generated via dbt_utils.date_spine macro.

  Why a date dimension?
  ---------------------
  Finance and demand mart models aggregate by month and day-of-week.  A date
  dimension ensures every calendar date has a row — even on days with zero
  trips — which prevents gaps in time-series charts and allows correct
  period-over-period comparisons.

  The dbt_utils.date_spine macro generates one row per date between
  start_date (inclusive) and end_date (exclusive).  We cover 2023-01-01
  through 2025-01-01 to include all three loaded months plus buffer.

  Materialization: table (small static table, referenced as FK in aggregations)
  transient: true — date spine never changes; Time Travel storage is unnecessary overhead.
*/

{{ config(transient = true) }}

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart   = "day",
            start_date = "cast('2023-01-01' as date)",
            end_date   = "cast('2025-01-01' as date)"
        )
    }}
),

dates as (
    select
        cast(date_day as date)                      as date_day,
        year(date_day)                              as year,
        month(date_day)                             as month_number,
        monthname(date_day)                         as month_name,
        quarter(date_day)                           as quarter,
        dayofweek(date_day)                         as day_of_week,       -- 1=Sun in Snowflake
        dayname(date_day)                           as day_name,
        dayofyear(date_day)                         as day_of_year,
        weekofyear(date_day)                        as week_of_year,

        -- ISO year-month string: used in mart GROUP BY and chart labels
        to_char(date_day, 'YYYY-MM')                as year_month,

        -- Convenience booleans
        (dayofweek(date_day) in (1, 7))             as is_weekend,
        (dayofweek(date_day) not in (1, 7))         as is_weekday

    from date_spine
)

select * from dates
