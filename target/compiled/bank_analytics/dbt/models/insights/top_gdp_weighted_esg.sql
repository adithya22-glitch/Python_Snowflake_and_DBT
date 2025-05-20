
SELECT
    "country",
    "country_name",
    "year",
    SUM("value" * "gdp") / SUM("gdp") AS gdp_weighted_esg
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE "gdp" IS NOT NULL AND "value" IS NOT NULL
GROUP BY "country", "country_name", "year"
ORDER BY gdp_weighted_esg DESC