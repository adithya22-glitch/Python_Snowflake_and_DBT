-- models/resilience_index.sql



SELECT
    country_name,
    AVG(value) / NULLIF(AVG(gdp), 0) AS resilience_index
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE gdp IS NOT NULL
GROUP BY country_name
ORDER BY resilience_index DESC