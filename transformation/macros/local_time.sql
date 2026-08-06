{#
    Local-time conversion helpers.

    Events are stored in UTC. Business questions are asked in LOCAL time:
    08:12 IST is 02:42 UTC, so bucketing hour-of-day on UTC would place the
    Bengaluru morning peak at 2 a.m. - and the resulting chart looks plausible
    enough to be rationalised rather than investigated.

    The session timezone is pinned to UTC in profiles.yml precisely so these
    conversions must be explicit. Without that pin, DuckDB inherits the machine
    timezone and `extract(hour FROM ts)` silently returns local hours on one
    machine and UTC hours on another.
#}

{% macro to_local(ts_column, tz_column='city.timezone') -%}
    ({{ ts_column }} at time zone {{ tz_column }})
{%- endmacro %}


{#  Date surrogate key in YYYYMMDD form, from LOCAL time. #}
{% macro local_date_key(ts_column, tz_column='city.timezone') -%}
    cast(strftime({{ to_local(ts_column, tz_column) }}, '%Y%m%d') as integer)
{%- endmacro %}


{#  Time-of-day surrogate key in HHMM form, from LOCAL time. #}
{% macro local_time_key(ts_column, tz_column='city.timezone') -%}
    cast(strftime({{ to_local(ts_column, tz_column) }}, '%H%M') as integer)
{%- endmacro %}


{% macro local_hour(ts_column, tz_column='city.timezone') -%}
    cast(extract(hour from {{ to_local(ts_column, tz_column) }}) as smallint)
{%- endmacro %}


{#
    Resolve a natural key to its surrogate, mapping misses to -1.

    NEVER a null foreign key. A null drops the row from every inner join
    silently - the loss appears in no count and no error log. The -1 UNKNOWN row
    keeps the fact, keeps the join working, and makes unknown values countable
    so a rising UNKNOWN rate becomes an alert (reference_data.md 0.2).
#}
{% macro unknown_key(column) -%}
    coalesce({{ column }}, -1)
{%- endmacro %}
