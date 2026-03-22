/*
  assert_no_future_pickups
  ------------------------
  Singular test: fails if any trip has a pickup_datetime in the future.

  A trip with a future pickup time is definitionally impossible — it means
  the source data has corrupt timestamps (system clock error, timezone bug,
  or data entry mistake).  Even one future pickup would indicate a data
  quality issue that could bias time-based analytics.

  This test returns the rows that VIOLATE the assertion.
  dbt singular tests pass when the query returns 0 rows.
*/

select
    trip_id,
    pickup_datetime,
    _source_month
from {{ ref('fct_trips') }}
where pickup_datetime > current_timestamp()
