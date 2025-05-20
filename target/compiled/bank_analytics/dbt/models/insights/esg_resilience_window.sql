
WITH ranked AS (
    SELECT
        "country_name",
        "indicator_name",
        "year",
        "value",
        STDDEV("value") OVER (PARTITION BY "country_name", "indicator_name" ORDER BY "year" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_std
    FROM BANK_DATA.ANALYTICS.stg_esg_risk
    WHERE "value" IS NOT NULL
)
SELECT * FROM ranked WHERE rolling_std IS NOT NULL