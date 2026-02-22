{% snapshot dim_ad_set %}
{{
    config(
        unique_key="ma_ad_set_id",
        strategy="timestamp",
        updated_at="updated_at",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    ma_ad_set_id,

    -- attributes
    ad_set_name,
    campaign_id,
    billing_event,
    optimization_goal,
    daily_budget,
    start_time,
    end_time,
    status,

    -- audit fields
    silver_load_date,
    coalesce(updated_at, silver_load_date) as updated_at

from {{ ref("ad_set") }}

{% endsnapshot %}