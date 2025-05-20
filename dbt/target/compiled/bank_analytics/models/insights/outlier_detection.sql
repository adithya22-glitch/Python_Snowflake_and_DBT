-- models/outlier_detection.sql



WITH stats AS (
    SELECT
        indicator_name,
        AVG(value) AS mean_val,
        STDDEV(value) AS std_val
    FROM BANK_DATA.ANALYTICS.stg_esg_risk
    GROUP BY indicator_name
)
SELECT
    s.country_name,
    s.indicator_name,
    s.year,
    s.value,
    (s.value - st.mean_val) / st.std_val AS z_score
FROM BANK_DATA.ANALYTICS.stg_esg_risk s
JOIN stats st
  ON s.indicator_name = st.indicator_name
WHERE ABS((s.value - st.mean_val) / st.std_val) > 2