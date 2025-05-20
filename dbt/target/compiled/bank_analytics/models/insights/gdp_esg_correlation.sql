-- models/gdp_esg_correlation.sql



SELECT
    indicator_name,
    CORR(value, gdp) AS gdp_esg_correlation
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE gdp IS NOT NULL
GROUP BY indicator_name
ORDER BY ABS(gdp_esg_correlation) DESC