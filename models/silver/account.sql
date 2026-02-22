{#
-- Description: Incremental Load Script for Silver Layer - account Table
-- Script Name: silver_account.sql
-- Created on: 17-feb-2026
-- Author: Sushil Kumar Kompally
-- Purpose:
--     Incremental load from Bronze to Silver for the account table.
-- Data source version: v1.0 (Adjust if needed)
-- Change History:
--     17-feb-2026 - Initial creation - Sushil Kompally
#}

{{ config(
    unique_key='ma_account_id',
    incremental_strategy='merge',
    materialized='table',
    pre_hook="{{ log_model_audit(status='STARTED') }}",
    post_hook="{{ log_model_audit(status='SUCCESS') }}"
) }}

with raw as (

  select
    *,
    {{ source_metadata() }}
  from {{ source('meta_ads_bronze', 'account') }}   -- change to your declared source if needed
  where 1=1
  {{ incremental_filter() }}

),

cleaned as (

  select
    -- ========== PK ==========
    {{ clean_string('accountid') }}                     as ma_account_id,

    -- ======== DETAILS ========
    {{ clean_string('accountname') }}                   as account_name,
    {{ clean_string('currency') }}                      as currency,
    {{ clean_string('timezone') }}                      as timezone,
    case when {{ clean_string('status') }} = 'DELETED' then true else false end as is_deleted,

    -- ========= DATES/TIMES =========
    {{ safe_date('updatedat') }}                   as updated_at,

    -- ========= AUDIT =========
    current_timestamp()::timestamp_ntz                 as silver_load_date

  from raw
),

final as (

  select
    -- PK
    ma_account_id,

    -- DETAILS
    account_name,
    currency,
    timezone,
    is_deleted,

    -- DATES
    updated_at,

    -- AUDIT
    silver_load_date

  from cleaned
)

select *
from final