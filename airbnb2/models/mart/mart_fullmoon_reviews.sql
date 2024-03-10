{{ config(
    materialized = 'table'
) }}

WITH fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),
full_moon_dates AS (
    SELECT * FROM {{ ref('full_moon') }}
)

SELECT
    r.*,
    CASE
        WHEN fm.full_moon_date IS NULL THEN 'not full moon'
        ELSE 'full moon'
    END AS is_full_moon
FROM
    fct_reviews r
LEFT JOIN
    full_moon_dates fm
ON
    TO_DATE(r.review_date, 'YYYY-MM-DD') = TO_DATE(fm.full_moon_date, 'YYYY-MM-DD') - INTERVAL '1 day'

