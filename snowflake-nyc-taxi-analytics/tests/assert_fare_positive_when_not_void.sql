/*
  assert_fare_positive_when_not_void
  -----------------------------------
  Singular test: fails if any non-voided, non-dispute trip has a fare_amount <= 0.

  Rationale:
    - Payment type 3 = No charge → fare_amount can legitimately be 0
    - Payment type 4 = Dispute   → fare may be 0 pending resolution
    - Payment type 6 = Voided    → fare should be 0
    - All other payment types    → a metered trip must have fare_amount > 0

  A zero or negative fare on a normal credit card or cash trip signals either
  a meter malfunction, a manual correction not properly recorded, or a data
  pipeline issue that silently zeroed out the fare column.

  Returns the violating rows.  Test passes when count = 0.
*/

select
    trip_id,
    payment_type,
    payment_type_name,
    fare_amount,
    total_amount,
    _source_month
from {{ ref('fct_trips') }}
where
    payment_type not in (3, 4, 6)   -- exclude no-charge, dispute, voided
    and fare_amount <= 0
