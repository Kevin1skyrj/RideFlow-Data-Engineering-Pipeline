{#
    Incremental window selection.

    Two modes, and which one applies is decided by whether backfill bounds were
    supplied:

    NORMAL RUN
      Reprocess everything that arrived since the last run, minus a lookback
      wider than the maximum tolerated lateness. The window is anchored on
      `dbt_loaded_at` in the TARGET table, so it advances on its own.

    BACKFILL
      Reprocess an explicit window, ignoring the high-water mark entirely.
      Combined with `delete+insert`, this makes re-running any historical range
      produce identical output - which is what makes backfill safe rather than
      a source of double-counting.

    The filter is applied to `landed_at` - the PHYSICAL load time - and never to
    an event timestamp or to `ingested_at`.

    Why not event time: the high-water mark would advance past a late-arriving
    event, which would then be missed permanently with no error (etl_design.md
    6.2).

    Why not ingested_at: it looks like an arrival clock but is a BUSINESS
    timestamp, set by the producer so that late arrivals are simulable. It is
    not monotonic with respect to the load. Re-consuming a topic re-lands old
    events today while they keep an ingested_at from months ago, so a lookback
    window based on it skips them entirely - and the failure is invisible,
    because dbt succeeds and the marts stay internally consistent. Measured on
    real data: 4,098 trips landed and never reached fct_trips.

    `landed_at` only ever moves forward, because the consumer stamps it at
    write time.
#}

{% macro incremental_window(timestamp_column) %}
    {%- set backfill_start = var('backfill_start', none) -%}
    {%- set backfill_end = var('backfill_end', none) -%}

    {%- if backfill_start and backfill_end -%}
        {#- Explicit backfill: bounded, repeatable, independent of run history. -#}
        {{ timestamp_column }} >= timestamptz '{{ backfill_start }}'
        and {{ timestamp_column }} < timestamptz '{{ backfill_end }}'

    {%- elif backfill_start or backfill_end -%}
        {#- Half a window is almost certainly a mistake, and silently ignoring
            it would process the wrong range while appearing to succeed. -#}
        {{ exceptions.raise_compiler_error(
            "backfill_start and backfill_end must be supplied together. Got start="
            ~ backfill_start ~ ", end=" ~ backfill_end
        ) }}

    {%- else -%}
        {#- Normal incremental run. -#}
        {{ timestamp_column }} >= (
            select coalesce(max(dbt_loaded_at), timestamptz '1970-01-01')
                 - interval {{ var('incremental_lookback_hours') }} hour
            from {{ this }}
        )
    {%- endif -%}
{% endmacro %}


{#
    True when this run is an explicit backfill. Used only for logging, so a run
    that reprocesses history says so rather than looking like a normal cycle.
#}
{% macro is_backfill() %}
    {{ return(var('backfill_start', none) is not none and var('backfill_end', none) is not none) }}
{% endmacro %}
