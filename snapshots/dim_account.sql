{% snapshot dim_account %}
{{
    config(
        unique_key="ma_account_id",
        strategy="timestamp",
        updated_at="updated_at",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    ma_account_id,

    -- attributes
    account_name,
    currency,
    timezone,

    -- audit fields from source
    silver_load_date,
    -- ensure movement even if source updated_at is occasionally null
    coalesce(updated_at, silver_load_date) as updated_at

from {{ ref("account") }}

{% endsnapshot %}