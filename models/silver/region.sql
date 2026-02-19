{#
-- Description: Incremental Load Script for Silver Layer - region Table
-- Script Name: silver_region.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the region table.
-- Data source version: v1.0 (Adjust if needed)
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_region_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads', 'region') }}   -- adjust to your declared bronze source/table if different
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== PK ==========
    {{ clean_string('regionid') }}       as ma_region_id,

    -- ======== DETAILS ========
    {{ clean_string('regionname') }}     as region_name,
    {{ clean_string('regioncode') }}     as region_code,
    {{ clean_string('countryid') }}      as country_id,
    {{ clean_string('status') }}         as status,

    -- ========= DATES/TIMES =========
    {{ safe_timestamp('updatedat') }}    as updated_at,

    -- ========= FLAGS =========
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========= AUDIT =========
    current_timestamp()::timestamp_ntz   as silver_load_date

  from raw
),

final as (
  select
    -- PK
    ma_region_id,

    -- DETAILS
    region_name,
    region_code,
    country_id,
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