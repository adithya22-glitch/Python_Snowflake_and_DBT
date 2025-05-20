{{ config(materialized='view') }}
SELECT * FROM RAW_DATA.BANK_ESG_RISK