-- models/top_esg_countries.sql



WITH ranked AS (
    SELECT
        country_name,
        AVG(value) AS avg_esg
    FROM BANK_DATA.ANALYTICS.stg_esg_risk
    GROUP BY country_name
)
SELECT *
FROM ranked
ORDER BY avg_esg DESC
LIMIT 10