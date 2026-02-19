{% snapshot dim_campaign %}
{{
    config(
        unique_key="ma_campaign_id",
        strategy="timestamp",
        updated_at="updated_at",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    ma_campaign_id,

    -- attributes
    campaign_name,
    account_id,
    status,
    objective,
    start_time,
    end_time,

    -- audit fields
    silver_load_date,
    coalesce(updated_at, silver_load_date) as updated_at

from {{ ref("campaign") }}

{% endsnapshot %}