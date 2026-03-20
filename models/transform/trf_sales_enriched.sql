{{ 
    config(
        materialized = 'incremental',
        unique_key = 'order_id',
        incremental_strategy = 'merge'
    ) 
}}

SELECT
    s.order_id,
    s.order_date,
    s.customer_id,
    s.product_id,
    s.store_id,
    s.quantity,
    s.unit_price,
    s.discount,
    s.payment_type,
    s.created_at,
    c.customer_name,
    c.gender,
    c.city AS customer_city,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.unit_cost,
    st.store_name,
    st.region,
    (s.quantity * s.unit_price) AS gross_sales,
    (s.quantity * s.unit_price * s.discount / 100) AS discount_amount,
    (s.quantity * s.unit_price) - (s.quantity * s.unit_price * s.discount / 100) AS net_sales,
    ((s.quantity * s.unit_price) - (s.quantity * p.unit_cost)) AS profit

FROM {{ ref('stg_sales') }} s 

LEFT JOIN {{ ref('stg_customers') }} c
    ON s.customer_id = c.customer_id

LEFT JOIN {{ ref('stg_products') }} p
    ON s.product_id = p.product_id

LEFT JOIN {{ ref('stg_stores') }} st
    ON s.store_id = st.store_id

{% if is_incremental() %}

WHERE s.created_at > (
    SELECT DATEADD(hour, -2, MAX(t.created_at))
    FROM {{ this }} t
)

{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.order_id
    ORDER BY s.created_at DESC
) = 1