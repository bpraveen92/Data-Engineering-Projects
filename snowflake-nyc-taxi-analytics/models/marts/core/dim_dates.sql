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
        cast(date_day as date)          as date_day,
        year(date_day)                  as year,
        month(date_day)                 as month_number,
        monthname(date_day)             as month_name,
        quarter(date_day)               as quarter,
        dayofweek(date_day)             as day_of_week,
        dayname(date_day)               as day_name,
        dayofyear(date_day)             as day_of_year,
        weekofyear(date_day)            as week_of_year,
        to_char(date_day, 'YYYY-MM')    as year_month,
        (dayofweek(date_day) in (1, 7)) as is_weekend,
        (dayofweek(date_day) not in (1, 7)) as is_weekday

    from date_spine
)

select * from dates
