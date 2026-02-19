{% snapshot dim_region %}
{{
    config(
        unique_key="ma_region_id",
        strategy="timestamp",
        updated_at="updated_at",
        pre_hook="{{ log_model_audit(status='started') }}",
        post_hook="{{ log_model_audit(status='success') }}"
    )
}}

select
    -- business/natural key
    ma_region_id,

    -- attributes
    region_name,
    region_code,
    country_id,

    -- audit fields
    silver_load_date,
    coalesce(updated_at, silver_load_date) as updated_at

from {{ ref("region") }}

{% endsnapshot %}