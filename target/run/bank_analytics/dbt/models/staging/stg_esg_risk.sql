
  create or replace   view BANK_DATA.ANALYTICS.stg_esg_risk
  
   as (
    
SELECT * FROM RAW_DATA.BANK_ESG_RISK
  );

