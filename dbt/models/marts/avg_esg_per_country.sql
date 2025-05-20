{{ config(materialized='view') }}
SELECT
    "country_name",
    AVG("value") AS avg_esg_score
FROM {{ ref('stg_esg_risk') }}
WHERE "value" IS NOT NULL
GROUP BY "country_name"
ORDER BY avg_esg_score DESC
