{#
-- Description: Incremental Load Script for Silver Layer - insights Table
-- Script Name: silver_insights.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze (insights) to Silver with daily grain metrics.
-- Data source version: v1.0
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_insights_sk',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (
  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads', 'insights') }}   -- << using insights table
  where 1=1
  {{ incremental_filter() }}
),

cleaned as (
  select
    -- ========== GRAIN / KEYS ==========
    /* Normalize date to DATE */
    to_date({{ safe_date('date') }})                    as ma_date,

    {{ clean_string('accountid') }}                          as account_id,
    {{ clean_string('campaignid') }}                         as campaign_id,
    {{ clean_string('adsetid') }}                            as ad_set_id,
    {{ clean_string('adid') }}                               as ad_id,
    {{ clean_string('regionid') }}                           as region_id,

    -- Surrogate key (daily grain x account x campaign x adset x ad x region)
    {{ dbt_utils.generate_surrogate_key([
    "accountid","campaignid","adsetid","adid","regionid"
    ]) }}                                                    as ma_insights_sk,

    -- ========== METRICS ==========
    {{ safe_number('impressions') }}                         as impressions,
    {{ safe_number('reach') }}                               as reach,
    {{ safe_number('clicks') }}                              as clicks,
    {{ safe_number('adclicks') }}                            as ad_clicks,
    {{ safe_number('cpc') }}                                 as cpc,
    {{ safe_number('cpm') }}                                 as cpm,
    {{ safe_number('ctr') }}                                 as ctr,
    {{ safe_number('spend') }}                               as spend,
    {{ safe_number('conversions') }}                         as conversions,
    {{ safe_number('purchasevalue') }}                       as purchase_value,

    -- ========== STATUS / DATES ==========
    {{ clean_string('status') }}                             as status,
    {{ safe_date('updatedat') }}                        as updated_at,

    -- ========== FLAGS ==========
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========== AUDIT ==========
    current_timestamp()::timestamp_ntz                       as silver_load_date

  from raw
),

final as (
  select
    -- KEYS
    ma_insights_sk,
    ma_date,
    account_id,
    campaign_id,
    ad_set_id,
    ad_id,
    region_id,

    -- METRICS
    impressions,
    reach,
    clicks,
    ad_clicks,
    cpc,
    cpm,
    ctr,
    spend,
    conversions,
    purchase_value,

    -- STATUS / DATES
    status,
    updated_at,

    -- FLAGS
    is_deleted,

    -- AUDIT
    silver_load_date

  from cleaned
)

select *
from final
