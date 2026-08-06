{{ config(severity='error') }}

/*
    Invariant F1: the fare components must sum to total_fare.

    severity = error, deliberately. Financial invariants BLOCK publication while
    other quality signals only warn: publishing a wrong revenue number is worse
    than publishing none (testing_strategy.md 4.5).

    Tolerance is one paisa, absorbing rounding at the final total only. Any
    wider drift is a real defect in the fare engine, not a rounding artefact.
*/

select
    trip_id,
    total_fare,
    fare_component_sum,
    abs(fare_component_sum - total_fare) as drift
from {{ ref('stg_ride_completed') }}
where abs(fare_component_sum - total_fare) > 0.01
