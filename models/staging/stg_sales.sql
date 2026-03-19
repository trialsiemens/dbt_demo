WITH source_data AS (
    SELECT
        CAST(ORDER_ID AS NUMBER)            AS order_id,
        CAST(ORDER_DATE AS DATE)            AS order_date,
        CAST(CUSTOMER_ID AS NUMBER)         AS customer_id,
        CAST(PRODUCT_ID AS NUMBER)          AS product_id,
        CAST(STORE_ID AS NUMBER)            AS store_id,
        COALESCE(QUANTITY, 1)               AS quantity,
        COALESCE(UNIT_PRICE, 0)             AS unit_price,
        COALESCE(DISCOUNT, 0)               AS discount,
        UPPER(TRIM(PAYMENT_TYPE))           AS payment_type
    FROM {{ source('raw','SALES') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_date DESC NULLS LAST
        ) AS rn
    FROM source_data
)
SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    store_id,
    quantity,
    unit_price,
    discount,
    payment_type
FROM deduplicated
WHERE rn = 1