{{ config(materialized='view') }}
 
SELECT
    CAST(date_key AS NUMBER)        AS date_key,
    CAST(full_date AS DATE)         AS full_date,
    year,
    month,
    day,
    day_name,
    month_name,
    quarter,
    week_of_year,
    is_weekend
FROM {{ source('raw','DATE') }}