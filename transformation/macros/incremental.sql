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

    The filter is always applied to an ARRIVAL timestamp (`ingested_at`), never
    an event timestamp. Arrival time is monotonic, so the high-water mark only
    ever moves forward. Filtering on event time would let the mark advance past
    a late-arriving event, which would then be missed PERMANENTLY, with no
    error and no gap in any log (etl_design.md 6.2).
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
