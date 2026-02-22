{#
-- Description: Incremental Load Script for Silver Layer - creative Table
-- Script Name: silver_creative.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the creative table.
-- Data source version: v1.0 (Adjust if needed)
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_creative_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads_bronze', 'creative') }}   -- adjust to your declared source/table if different
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== PK ==========
    {{ clean_string('creativeid') }}     as ma_creative_id,

    -- ======== DETAILS ========
    {{ clean_string('name') }}           as name,
    {{ clean_string('title') }}          as title,
    {{ clean_string('body') }}           as body,
    {{ clean_string('imageurl') }}       as image_url,
    {{ clean_string('videourl') }}       as video_url,
    {{ clean_string('url') }}            as url,
    {{ clean_string('status') }}         as status,

    -- ========= DATES/TIMES =========
    {{ safe_date('updatedat') }}    as updated_at,

    -- ========= FLAGS =========
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========= AUDIT =========
    current_timestamp()::timestamp_ntz   as silver_load_date

  from raw
),

final as (
  select
    -- PK
    ma_creative_id,

    -- DETAILS
    name,
    title,
    body,
    image_url,
    video_url,
    url,
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