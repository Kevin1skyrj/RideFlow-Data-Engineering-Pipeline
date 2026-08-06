{{ config(severity='warn') }}

/*
    Sequence rule S3: PaymentCompleted requires a preceding RideCompleted.

    severity = warn, and the reasoning matters.

    A payment without a completion is NOT a pipeline defect. It is the expected
    consequence of the RideCompleted event being quarantined in the DLQ: the
    payment arrived and validated normally, its predecessor did not. Measured on
    real data, 11 trips are in this state, accounting for exactly Rs5,712.98 of
    apparent revenue gap.

    Erroring here would mean the DLQ can never be exercised without failing the
    entire warehouse build - the quality gate punishing the pipeline for working
    as designed, exactly as with the orphaned DriverOffline in staging.

    But it must not be silent either: these payments represent money collected
    for trips the warehouse cannot describe. The count is surfaced in
    fct_pipeline_quality so it can be tracked, and a sudden rise means the DLQ
    rate is rising.
*/

select
    t.trip_id,
    p.payment_id,
    p.amount_charged,
    'payment exists but RideCompleted was never landed' as reason
from {{ ref('fct_payments') }} as p
join {{ ref('fct_trips') }} as t using (trip_id)
where t.completed_at is null
