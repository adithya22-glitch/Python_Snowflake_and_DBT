-- models/dim_country_stats.sql



SELECT
    country,
    country_name,
    COUNT(DISTINCT indicator_name) AS num_indicators,
    COUNT(DISTINCT year) AS num_years,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM BANK_DATA.ANALYTICS.stg_esg_risk
GROUP BY country, country_name