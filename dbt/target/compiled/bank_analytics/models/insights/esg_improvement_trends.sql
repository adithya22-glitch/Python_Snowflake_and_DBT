-- models/esg_improvement_trends.sql



WITH first_values AS (
    SELECT
        country_name,
        indicator_name,
        MIN(year) AS first_year
    FROM BANK_DATA.ANALYTICS.stg_esg_risk
    GROUP BY country_name, indicator_name
),
last_values AS (
    SELECT
        country_name,
        indicator_name,
        MAX(year) AS last_year
    FROM BANK_DATA.ANALYTICS.stg_esg_risk
    GROUP BY country_name, indicator_name
),
first_data AS (
    SELECT a.country_name, a.indicator_name, s.value AS first_value
    FROM first_values a
    JOIN BANK_DATA.ANALYTICS.stg_esg_risk s
      ON a.country_name = s.country_name AND a.indicator_name = s.indicator_name AND a.first_year = s.year
),
last_data AS (
    SELECT a.country_name, a.indicator_name, s.value AS last_value
    FROM last_values a
    JOIN BANK_DATA.ANALYTICS.stg_esg_risk s
      ON a.country_name = s.country_name AND a.indicator_name = s.indicator_name AND a.last_year = s.year
)
SELECT
    f.country_name,
    f.indicator_name,
    l.last_value - f.first_value AS improvement
FROM first_data f
JOIN last_data l
  ON f.country_name = l.country_name AND f.indicator_name = l.indicator_name
ORDER BY improvement DESC