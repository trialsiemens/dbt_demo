{{ config(materialized='view') }}

WITH source_data AS (

    SELECT
        CAST(CUSTOMER_ID AS NUMBER)                    AS customer_id,
        TRIM(FIRST_NAME)                               AS first_name,
        TRIM(LAST_NAME)                                AS last_name,
        TRIM(FIRST_NAME) || ' ' || TRIM(LAST_NAME)     AS customer_name,
        LOWER(TRIM(EMAIL))                             AS email,
        UPPER(TRIM(GENDER))                            AS gender,
        CAST(DATE_OF_BIRTH AS DATE)                    AS date_of_birth,
        INITCAP(TRIM(CITY))                            AS city,
        UPPER(TRIM(STATE))                             AS state,
        UPPER(TRIM(COUNTRY))                           AS country,
        CAST(CREATED_AT AS TIMESTAMP)                  AS created_at,
        CAST(UPDATED_AT AS TIMESTAMP)                  AS updated_at
    FROM {{ source('raw', 'CUSTOMERS') }}
),
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY updated_at DESC NULLS LAST
        ) AS rn
    FROM source_data
)
SELECT
    customer_id,
    first_name,
    last_name,
    customer_name,
    email,
    gender,
    date_of_birth,
    city,
    state,
    country,
    created_at,
    updated_at
FROM deduplicated
WHERE rn = 1