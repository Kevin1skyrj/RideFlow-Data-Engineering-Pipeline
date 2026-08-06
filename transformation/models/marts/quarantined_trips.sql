{{ config(materialized='view', tags=['marts','quality']) }}

/*
    Trips excluded from business analysis, with the reason.

    QUARANTINE, NEVER DISCARD. A quarantined trip stays in fct_trips and stays
    listed here. If its missing event arrives late, the next run reassembles it
    correctly and it leaves quarantine on its own. A discarded trip could never
    recover, because the evidence it existed would be gone.

    Consumers of fct_trips filter on `is_quarantined = false` for business
    metrics. This view exists so the excluded rows are inspectable rather than
    invisible - a silent exclusion is indistinguishable from data loss.
*/

select
    trip_id,
    ride_status_id,
    requested_at,
    total_fare,

    not has_request_event                        as missing_request_event,
    not is_sequence_valid                        as invalid_sequence,

    case
        when not has_request_event
            then 'RideRequested never landed - trip has no anchor, so request '
                 || 'attributes (tier, surge, zones) are unknown'
        when not is_sequence_valid
            then 'event sequence violates rules S1-S8 - the assembled trip is '
                 || 'internally inconsistent'
        else 'unknown'
    end                                          as quarantine_reason,

    event_count,
    max_lateness_sec

from {{ ref('fct_trips') }}
where is_quarantined
