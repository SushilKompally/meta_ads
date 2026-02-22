{#
-- Description: Incremental Load Script for Silver Layer - ad_set Table
-- Script Name: silver_ad_set.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the ad_set table.
-- Data source version: v1.0
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_ad_set_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads_bronze', 'ad_set') }}  -- adjust if your bronze table name differs
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== PK ==========
    {{ clean_string('adsetid') }}             as ma_ad_set_id,

    -- ========= DETAILS =========
    {{ clean_string('adsetname') }}           as ad_set_name,
    {{ clean_string('campaignid') }}          as campaign_id,
    {{ clean_string('billingevent') }}        as billing_event,
    {{ clean_string('optimizationgoal') }}    as optimization_goal,
    {{ clean_string('dailybudget') }}          as daily_budget,

    -- ========= DATES/TIMES =========
    {{ safe_date('starttime') }}              as start_time,
    {{ safe_date('endtime') }}                as end_time,
    {{ safe_date('updatedat') }}              as updated_at,

    -- ========= STATUS =========
    {{ clean_string('status') }}              as status,

    -- ========= FLAGS =========
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========= AUDIT =========
    current_timestamp()::timestamp_ntz        as silver_load_date

  from raw
),

final as (
  select
    -- PK
    ma_ad_set_id,

    -- DETAILS
    ad_set_name,
    campaign_id,
    billing_event,
    optimization_goal,
    daily_budget,

    -- DATES
    start_time,
    end_time,
    updated_at,

    -- STATUS
    status,

    -- FLAGS
    is_deleted,

    -- AUDIT
    silver_load_date

  from cleaned
)

select *
from final