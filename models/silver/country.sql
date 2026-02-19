{#
-- Description: Incremental Load Script for Silver Layer - country Table
-- Script Name: silver_country.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the country table.
-- Data source version: v1.0 (Adjust if needed)
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_country_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads', 'country') }}   -- adjust to your declared source/table if different
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== PK ==========
    {{ clean_string('countryid') }}       as ma_country_id,

    -- ======== DETAILS ========
    {{ clean_string('countryname') }}     as country_name,
    {{ clean_string('countrycode') }}     as country_code,
    {{ clean_string('currency') }}        as currency,
    {{ clean_string('timezone') }}        as timezone,
    {{ clean_string('status') }}          as status,

    -- ========= DATES/TIMES =========
    {{ safe_timestamp('updatedat') }}     as updated_at,

    -- ========= FLAGS =========
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========= AUDIT =========
    current_timestamp()::timestamp_ntz    as silver_load_date

  from raw
),

final as (
  select
    -- PK
    ma_country_id,

    -- DETAILS
    country_name,
    country_code,
    currency,
    timezone,
    status,

    -- DATES
    updated_at,

    -- FLAGS
    is_deleted,

    -- AUDIT
    silver_load_date

  from cleaned
)

select *
from final