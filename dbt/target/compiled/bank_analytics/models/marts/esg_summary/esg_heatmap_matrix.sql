-- models/esg_heatmap_matrix.sql



SELECT
    country_name,
    indicator_name,
    AVG(value) AS avg_esg
FROM BANK_DATA.ANALYTICS.stg_esg_risk
GROUP BY country_name, indicator_name