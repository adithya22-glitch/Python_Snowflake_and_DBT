
  create or replace   view BANK_DATA.ANALYTICS.esg_category_aggregation
  
   as (
    
SELECT
    "country_name",
    "year",
    CASE
        WHEN LOWER("indicator_name") LIKE '%environment%' THEN 'Environmental'
        WHEN LOWER("indicator_name") LIKE '%social%' THEN 'Social'
        WHEN LOWER("indicator_name") LIKE '%governance%' THEN 'Governance'
        ELSE 'Other'
    END AS esg_category,
    AVG("value") AS avg_score
FROM BANK_DATA.ANALYTICS.stg_esg_risk
WHERE "value" IS NOT NULL
GROUP BY "country_name", "year", esg_category
  );

