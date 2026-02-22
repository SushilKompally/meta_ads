{% snapshot dim_ad %}
{{
    config(
        unique_key="ma_ad_id",
        strategy="timestamp",
        updated_at="updated_at",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    ma_ad_id,

    -- attributes (from ad model)
    ad_name,
    ad_set_id,
    campaign_id,
    creative_id,
    status,

    -- audit fields
    silver_load_date,

    -- timestamp used by the timestamp strategy
    coalesce(updated_at, silver_load_date) as updated_at

from {{ ref("ad") }}

{% endsnapshot %}