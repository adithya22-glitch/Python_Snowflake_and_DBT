-- models/esg_vs_gdp.sql



SELECT
    country_name,
    indicator_name,
    AVG(value) AS avg_esg,
    AVG(gdp) AS avg_gdp
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE gdp IS NOT NULL
GROUP BY country_name, indicator_name