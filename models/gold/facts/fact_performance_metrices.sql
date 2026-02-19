{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='fact_perf_sk',
    on_schema_change='append_new_columns',
    pre_hook="{{ log_model_audit(status='started') }}",
    post_hook="{{ log_model_audit(status='success') }}"
  ) 
}}

with final as (

  select
      -- ========= KEYS / GRAIN =========
      {{ dbt_utils.generate_surrogate_key([
       
        "i.account_id","i.campaign_id","i.ad_set_id","i.ad_id","i.region_id"
      ]) }}                                       as fact_perf_sk,

      -- grain columns (helpful for downstream joins and debugging)
      i.ma_date,
      i.account_id,
      i.campaign_id,
      i.ad_set_id,
      i.ad_id,
      i.region_id,

      -- ========= METRICS =========
      i.impressions,
      i.reach,
      i.clicks,
      i.ad_clicks,
      i.cpc,
      i.cpm,
      i.ctr,
      i.spend,
      i.conversions,
      i.purchase_value,

      -- ========= WATERMARK / AUDIT =========
      i.updated_at,
      current_timestamp()::timestamp_ntz           as gold_load_date

  from {{ ref('insights') }} i
  -- Ensure referential integrity by joining only to current dim rows
  inner join {{ ref('dim_account') }}  da
    on da.ma_account_id  = i.account_id  and da.is_current = true
  inner join {{ ref('dim_campaign') }} dc
    on dc.ma_campaign_id = i.campaign_id and dc.is_current = true
  inner join {{ ref('dim_ad_set') }}   das
    on das.ma_ad_set_id   = i.ad_set_id  and das.is_current = true
  inner join {{ ref('dim_ad') }}       d_ad
    on d_ad.ma_ad_id      = i.ad_id      and d_ad.is_current = true
  inner join {{ ref('dim_region') }}   dr
    on dr.ma_region_id    = i.region_id  and dr.is_current = true

  {% if is_incremental() %}
    -- process only new/updated insight rows beyond the target watermark
    where i.updated_at >
      (
        select coalesce(
          max(tgt.updated_at),
          '1900-01-01'::timestamp_ntz
        )
        from {{ this }} tgt
      )
  {% endif %}
)

select
  fact_perf_sk,
  ma_date,
  account_id,
  campaign_id,
  ad_set_id,
  ad_id,
  region_id,
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
  updated_at,
  gold_load_date
from final