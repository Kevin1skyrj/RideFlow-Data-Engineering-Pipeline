{{ config(severity='error') }}

/*
    Invariant N3: fare recorded on the trip must equal fare recorded on the
    payment, for every trip that has both.

    Scoped to trips holding BOTH records. A payment whose RideCompleted was
    quarantined in the DLQ has no trip-side fare to compare against - that is a
    completeness problem (covered by assert_s3_payment_requires_completion),
    not a reconciliation one. Conflating them would make this test fire on data
    that is arithmetically perfect.

    Measured per trip rather than as two grand totals: offsetting errors can
    make aggregate sums agree while individual rows are wrong.
*/

select
    t.trip_id,
    t.total_fare      as trip_side,
    p.trip_fare       as payment_side,
    abs(t.total_fare - p.trip_fare) as drift
from {{ ref('fct_trips') }} as t
join {{ ref('fct_payments') }} as p using (trip_id)
where t.total_fare is not null
  and abs(t.total_fare - p.trip_fare) > 0.01
