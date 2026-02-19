{#
-- Description: Incremental Load Script for Silver Layer - campaign Table
-- Script Name: silver_campaign.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the campaign table.
-- Data source version: v1.0 (Adjust if needed)
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_campaign_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads', 'campaign') }}   -- adjust to your declared source/table if different
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== PK ==========
    {{ clean_string('campaignid') }}     as ma_campaign_id,

    -- ======== DETAILS ========
    {{ clean_string('campaignname') }}   as campaign_name,
    {{ clean_string('accountid') }}      as account_id,
    {{ clean_string('status') }}         as status,
    {{ clean_string('objective') }}      as objective,

    -- ========= DATES/TIMES =========
    {{ safe_timestamp('starttime') }}    as start_time,
    {{ safe_timestamp('endtime') }}      as end_time,
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
    ma_campaign_id,

    -- DETAILS
    campaign_name,
    account_id,
    status,
    objective,

    -- DATES
    start_time,
    end_time,
    updated_at,

    -- FLAGS
    is_deleted,

    -- AUDIT
    silver_load_date

  from cleaned
)

select *
from final