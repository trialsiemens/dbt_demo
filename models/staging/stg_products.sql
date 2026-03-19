{{ config(materialized='view') }}


WITH source_data AS (
    SELECT
        CAST(PRODUCT_ID AS NUMBER)              AS product_id,
        TRIM(PRODUCT_NAME)                      AS product_name,
        UPPER(TRIM(CATEGORY))                   AS category,
        UPPER(TRIM(SUB_CATEGORY))               AS sub_category,
        UPPER(TRIM(BRAND))                      AS brand,
        COALESCE(UNIT_COST, 0)                  AS unit_cost,
        CAST(CREATED_AT AS TIMESTAMP)           AS created_at,
        CAST(UPDATED_AT AS TIMESTAMP)           AS updated_at
    FROM {{ source('raw','PRODUCTS') }}
),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY updated_at DESC NULLS LAST
        ) AS rn
    FROM source_data
)

SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    unit_cost,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1