/*
  assert_trip_duration_reasonable
  ---------------------------------
  Singular test: fails if any trip has a duration outside the 1–300 minute range.

  The staging model already filters rows where dropoff <= pickup (which would
  produce zero or negative durations).  This test guards the fact table against:

    - Duration = 0 slipping through due to timestamp rounding
    - Extremely long durations (> 5 hours) that are almost certainly meter
      left-on errors (driver forgot to end the trip) rather than real trips

  NYC taxi trips are almost always under 2 hours.  The 300-minute ceiling
  allows for rare edge cases (storm delays, long Island runs) while catching
  obvious phantom trips.

  Returns the violating rows.  Test passes when count = 0.
*/

select
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    pickup_zone,
    dropoff_zone,
    _source_month
from {{ ref('fct_trips') }}
where trip_duration_minutes not between 1 and 300
