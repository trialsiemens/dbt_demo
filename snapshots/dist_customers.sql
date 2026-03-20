{% snapshot dist_customers %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT
    customer_id,
    customer_name,
    email,
    gender,
    city,
    state,
    country,
    updated_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}