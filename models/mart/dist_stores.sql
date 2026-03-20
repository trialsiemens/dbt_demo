{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge'
) }}

SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    created_at,
    updated_at
FROM {{ ref('stg_stores') }}
