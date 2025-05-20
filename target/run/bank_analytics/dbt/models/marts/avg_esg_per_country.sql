
  create or replace   view BANK_DATA.ANALYTICS.avg_esg_per_country
  
   as (
    
SELECT
    "country_name",
    AVG("value") AS avg_esg_score
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE "value" IS NOT NULL
GROUP BY "country_name"
ORDER BY avg_esg_score DESC
  );

