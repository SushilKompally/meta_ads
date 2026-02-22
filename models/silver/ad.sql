{#
-- Description: Incremental Load Script for Silver Layer - ad Table
-- Script Name: silver_ad.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the ad table.
-- Data source version: v1.0 (Adjust if needed)
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_ad_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads_bronze', 'ad') }}   -- adjust if your bronze table name differs
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== PK ==========
    {{ clean_string('adid') }}         as ma_ad_id,

    -- ======== DETAILS ========
    {{ clean_string('adname') }}       as ad_name,
    {{ clean_string('adsetid') }}      as ad_set_id,
    {{ clean_string('campaignid') }}   as campaign_id,
    {{ clean_string('creativeid') }}   as creative_id,
    {{ clean_string('status') }}       as status,

    -- ========= DATES/TIMES =========
    {{ safe_date('updatedat') }}  as updated_at,

    -- ========= FLAGS =========
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========= AUDIT =========
    current_timestamp()::timestamp_ntz as silver_load_date

  from raw
),

final as (
  select
    -- PK
    ma_ad_id,

    -- DETAILS
    ad_name,
    ad_set_id,
    campaign_id,
    creative_id,
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