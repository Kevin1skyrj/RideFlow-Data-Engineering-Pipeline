{#
    Extraction helpers for the `payload_json` column.

    The landing zone stores each event's payload as JSON rather than flattened
    columns: nine event types have nine different shapes, and flattening them
    into one table would produce a very wide, very sparse structure. Staging
    extracts typed columns per event type, where the shape IS known.

    Every helper casts explicitly. Relying on DuckDB's implicit conversion would
    silently produce DOUBLE for money, and floating-point money is how a
    reconciliation ends up off by a paisa.
#}

{% macro payload_text(field) -%}
    json_extract_string({{ 'payload_json' }}, '$.{{ field }}')
{%- endmacro %}


{% macro payload_uuid(field) -%}
    json_extract_string(payload_json, '$.{{ field }}')
{%- endmacro %}


{% macro payload_int(field) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as integer)
{%- endmacro %}


{% macro payload_bigint(field) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as bigint)
{%- endmacro %}


{#
    Money. decimal(12,2), never DOUBLE.

    try_cast rather than cast: a malformed value should surface as NULL and be
    caught by a not_null test, rather than aborting the entire model run. One bad
    row must not take down the warehouse build.
#}
{% macro payload_money(field) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as decimal(12, 2))
{%- endmacro %}


{% macro payload_decimal(field, precision=10, scale=2) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as decimal({{ precision }}, {{ scale }}))
{%- endmacro %}


{#  Coordinates need 6 decimal places - roughly 0.1 m at the equator. #}
{% macro payload_coordinate(field) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as decimal(9, 6))
{%- endmacro %}


{% macro payload_bool(field) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as boolean)
{%- endmacro %}


{% macro payload_timestamp(field) -%}
    try_cast(json_extract_string(payload_json, '$.{{ field }}') as timestamp with time zone)
{%- endmacro %}


{#
    Empty string -> NULL.

    A producer that sends "" for an absent optional field must not create a
    value that is neither present nor null - that is a third state downstream
    logic will not handle (etl_design.md 8.2).
#}
{% macro payload_text_nullable(field) -%}
    nullif(json_extract_string(payload_json, '$.{{ field }}'), '')
{%- endmacro %}
