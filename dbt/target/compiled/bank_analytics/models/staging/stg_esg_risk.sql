

SELECT
    "country",
    "country_name",
    "indicator",
    "indicator_name",
    TRY_CAST("year" AS INT) AS year,
    TRY_CAST("value" AS FLOAT) AS value,
    TRY_CAST("gdp" AS FLOAT) AS gdp,
    TRY_CAST("population" AS FLOAT) AS population
FROM BANK_DATA.RAW_DATA.bank_esg_risk
WHERE "value" IS NOT NULL