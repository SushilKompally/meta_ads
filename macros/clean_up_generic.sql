
{# ---------------------------------------------------------
# Strings: trim + NULL if empty
# --------------------------------------------------------- #}
{% macro clean_string(col, tool_name='snowflake') -%}
nullif(trim(to_varchar({{ col }})), '')
{%- endmacro %}

{% macro clean_string_lower(col, tool_name='snowflake') -%}
lower(nullif(trim(to_varchar({{ col }})), ''))
{%- endmacro %}


{# ---------------------------------------------------------
# Numerics: safe integer/decimal
# --------------------------------------------------------- #}
{% macro safe_integer(col, precision=38, scale=3, tool_name='snowflake') -%}
try_to_number({{ col }}, {{ precision }}, {{ scale }})
{%- endmacro %}

{% macro safe_decimal(col, precision=18, scale=2, tool_name='snowflake') -%}
try_to_decimal({{ col }}, {{ precision }}, {{ scale }})
{%- endmacro %}


{# ---------------------------------------------------------
# Date: guard blanks/invalid and cutoff (Snowflake)
# Usage:
#   {{ safe_date('my_col') }}
#   {{ safe_date('my_col', cutoff_date='1900-01-01', fmt='YYYY-MM-DD') }}
# --------------------------------------------------------- #}
{% macro safe_date(col, cutoff_date='1900-01-01', fmt=None) -%}
case
  -- blank/NULL guard
  when {{ col }} is null then null
  when nullif(trim(cast({{ col }} as varchar)), '') is null then null

  -- parse to DATE safely (normalize to varchar first to avoid TIMESTAMP→DATE try_cast issues)
  when try_to_date(cast({{ col }} as varchar){% if fmt %}, '{{ fmt }}'{% endif %}) is null then null

  -- cutoff
  when try_to_date(cast({{ col }} as varchar){% if fmt %}, '{{ fmt }}'{% endif %}) 
       <= to_date('{{ cutoff_date }}') then null

  else try_to_date(cast({{ col }} as varchar){% if fmt %}, '{{ fmt }}'{% endif %})
end
{%- endmacro %}


{# ---------------------------------------------------------
# Timestamp (TIMESTAMP_NTZ): guard blanks/invalid (Snowflake)
# Usage:
#   {{ safe_timestamp_ntz('my_col') }}
#   {{ safe_timestamp_ntz('my_col', fmt='YYYY-MM-DD HH24:MI:SS') }}
# --------------------------------------------------------- #}
{% macro safe_timestamp_ntz(col, fmt=None) -%}
case
  -- blank/NULL guard
  when {{ col }} is null then null
  when nullif(trim(cast({{ col }} as varchar)), '') is null then null

  -- safe parse via varchar normalization
  when try_to_timestamp_ntz(cast({{ col }} as varchar){% if fmt %}, '{{ fmt }}'{% endif %}) is null then null

  else try_to_timestamp_ntz(cast({{ col }} as varchar){% if fmt %}, '{{ fmt }}'{% endif %})
end
{%- endmacro %}


{# ---------------------------------------------------------
# Number: safe numeric cast with optional default (Snowflake)
# Usage:
#   {{ safe_number('col_name') }}
#   {{ safe_number('col_name', precision=38, scale=6) }}
#   {{ safe_number('col_name', default_value=0) }}
#   {{ safe_number('col_name', precision=18, scale=2, default_value=0) }}
# Notes:
#   - Uses Snowflake try_to_number to avoid hard errors.
#   - Trims and treats empty strings as NULL.
#   - Avoids try_cast entirely.
# --------------------------------------------------------- #}
{% macro safe_number(col, precision=38, scale=6, default_value=None, tool_name='snowflake') -%}
case
  when {{ col }} is null then {{ default_value if default_value is not none else 'null' }}
  when nullif(trim(cast({{ col }} as varchar)), '') is null then {{ default_value if default_value is not none else 'null' }}
  else
    {% if default_value is not none -%}
      coalesce(try_to_number({{ col }}, {{ precision }}, {{ scale }}), {{ default_value }})
    {%- else -%}
      try_to_number({{ col }}, {{ precision }}, {{ scale }})
    {%- endif %}
end
{%- endmacro %}