{% snapshot dim_country %}
{{
    config(
        unique_key="ma_country_id",
        strategy="timestamp",
        updated_at="updated_at",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    ma_country_id,

    -- attributes
    country_name,
    country_code,
    currency,
    timezone,

    -- audit fields
    silver_load_date,
    coalesce(updated_at, silver_load_date) as updated_at

from {{ ref("country") }}

{% endsnapshot %}