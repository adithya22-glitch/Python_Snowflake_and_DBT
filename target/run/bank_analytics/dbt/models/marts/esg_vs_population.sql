
  create or replace   view BANK_DATA.ANALYTICS.esg_vs_population
  
   as (
    
SELECT
    "country",
    "country_name",
    "year",
    "value" / NULLIF("population", 0) AS esg_per_capita
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE "value" IS NOT NULL AND "population" IS NOT NULL
ORDER BY esg_per_capita DESC
  );

