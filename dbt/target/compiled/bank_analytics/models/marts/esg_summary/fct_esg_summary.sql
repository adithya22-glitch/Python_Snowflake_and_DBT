-- models/fct_esg_summary.sql



SELECT
    country,
    year,
    indicator_name,
    AVG(value) AS avg_value
FROM BANK_DATA.ANALYTICS.stg_esg_risk
GROUP BY country, year, indicator_name