-- models/indicator_volatility.sql



SELECT
    indicator_name,
    STDDEV(value) AS volatility
FROM BANK_DATA.ANALYTICS.stg_esg_risk
GROUP BY indicator_name
ORDER BY volatility DESC