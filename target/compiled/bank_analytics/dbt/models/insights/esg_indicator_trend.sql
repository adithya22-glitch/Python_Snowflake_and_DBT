
SELECT
    "country_name",
    "indicator_name",
    "year",
    "value",
    "value" - LAG("value") OVER (PARTITION BY "country_name", "indicator_name" ORDER BY "year") AS year_over_year_change
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE "value" IS NOT NULL