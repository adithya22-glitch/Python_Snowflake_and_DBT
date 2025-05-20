-- models/esg_per_capita.sql



SELECT
    country_name,
    SUM(value) / AVG(population) AS esg_per_capita
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE population IS NOT NULL
GROUP BY country_name