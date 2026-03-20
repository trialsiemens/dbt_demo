WITH source_data AS (
    SELECT
        CAST(STORE_ID AS NUMBER)                 AS store_id,
        TRIM(STORE_NAME)                         AS store_name,
        INITCAP(TRIM(CITY))                      AS city,
        UPPER(TRIM(STATE))                       AS state,
        UPPER(TRIM(REGION))                      AS region,
        CAST(CREATED_AT AS TIMESTAMP)            AS created_at,
        CAST(UPDATED_AT AS TIMESTAMP)            AS updated_at
    FROM {{ source('raw', 'STORES') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY updated_at DESC NULLS LAST
        ) AS rn
    FROM source_data
)
SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1