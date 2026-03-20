{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}
SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    store_id,
    TO_NUMBER(TO_CHAR(order_date,'YYYYMMDD')) AS date_key,
    quantity,
    unit_price,
    discount,

    gross_sales,
    discount_amount,
    net_sales,
    profit,

    created_at

FROM {{ ref('trf_sales_enriched') }}

{% if is_incremental() %}

WHERE created_at > (
    SELECT DATEADD(hour, -2, MAX(created_at))
    FROM {{ this }}
)

{% endif %}