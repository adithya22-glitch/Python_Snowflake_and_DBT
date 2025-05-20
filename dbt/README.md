# DBT ESG Analytics Project

This DBT project is designed to analyze and model ESG (Environmental, Social, and Governance) indicators across countries using Snowflake as the data warehouse. The models transform raw ESG data into meaningful, insightful, and actionable analytics.

---

## Project Structure

```
├── dbt_project.yml              # DBT configuration
├── models/
│   ├── staging/                 # Raw source models
│   ├── marts/                   # Core business logic & aggregations
│   │   └── esg_summary/         # Specific ESG category aggregations
│   └── insights/                # Analytical & advanced metrics
```

---

## Models Overview

### Staging

#### `stg_esg_risk.sql`
```sql
SELECT * FROM RAW_DATA.BANK_ESG_RISK
```
**Purpose**: Loads raw ESG, GDP, and population data from the Snowflake raw layer as-is. This model is the foundational staging layer used by all downstream transformations.

---

### Marts

#### `avg_esg_per_country.sql`
```sql
SELECT
  "country_name",
  AVG("value") AS avg_esg_score
FROM {{ ref('stg_esg_risk') }}
WHERE "value" IS NOT NULL
GROUP BY "country_name"
ORDER BY avg_esg_score DESC;
```
**Purpose**: Computes the average ESG score per country across all indicators and years. Useful for general country ranking.

#### `esg_vs_population.sql`
```sql
SELECT
  "country",
  "country_name",
  "year",
  "value" / NULLIF("population", 0) AS esg_per_capita
FROM {{ ref('stg_esg_risk') }}
WHERE "value" IS NOT NULL AND "population" IS NOT NULL
ORDER BY esg_per_capita DESC;
```
**Purpose**: Normalizes ESG scores on a per capita basis to compare countries more fairly regardless of population size.

#### `esg_category_aggregation.sql`
```sql
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
FROM {{ ref('stg_esg_risk') }}
WHERE "value" IS NOT NULL
GROUP BY "country_name", "year", esg_category;
```
**Purpose**: Groups ESG indicators into ESG categories and computes average scores per country and year for each category. Enables category-level ESG analysis.

---

### Insights

#### `esg_indicator_trend.sql`
```sql
SELECT
  "country_name",
  "indicator_name",
  "year",
  "value",
  "value" - LAG("value") OVER (PARTITION BY "country_name", "indicator_name" ORDER BY "year") AS year_over_year_change
FROM {{ ref('stg_esg_risk') }}
WHERE "value" IS NOT NULL;
```
**Purpose**: Calculates the year-over-year change in ESG values for each indicator by country. Useful for detecting improvements or regressions in ESG metrics.

#### `esg_resilience_window.sql`
```sql
WITH ranked AS (
  SELECT
    "country_name",
    "indicator_name",
    "year",
    "value",
    STDDEV("value") OVER (PARTITION BY "country_name", "indicator_name" ORDER BY "year" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_std
  FROM {{ ref('stg_esg_risk') }}
  WHERE "value" IS NOT NULL
)
SELECT * FROM ranked WHERE rolling_std IS NOT NULL;
```
**Purpose**: Calculates the standard deviation (volatility) of ESG scores over a rolling 3-year window. This is a proxy for stability or resilience of ESG performance.

#### `top_gdp_weighted_esg.sql`
```sql
SELECT
  "country",
  "country_name",
  "year",
  SUM("value" * "gdp") / SUM("gdp") AS gdp_weighted_esg
FROM {{ ref('stg_esg_risk') }}
WHERE "gdp" IS NOT NULL AND "value" IS NOT NULL
GROUP BY "country", "country_name", "year"
ORDER BY gdp_weighted_esg DESC;
```
**Purpose**: Weighs ESG performance by GDP to highlight sustainability efforts of economically influential countries.

---

## YML Files Overview

- `sources.yml`: Defines the `RAW_DATA.BANK_ESG_RISK` table as a source.
- `marts.yml`: Describes models like `avg_esg_per_country` and `esg_vs_population` with optional column tests and descriptions.
- `esg_summary.yml`: Adds metadata for `esg_category_aggregation`.
- `insights.yml`: Documents the purpose of indicator trend, resilience, and GDP-weighted models.

---

## How to Run the Project

From the root of your DBT project (where `dbt_project.yml` is located):

### 1. Install dependencies and prepare environment
```bash
dbt clean
```

### 2. Compile the project
```bash
dbt compile
```

### 3. Run all models
```bash
dbt run
```

### 4. Run all tests (optional)
```bash
dbt test
```

### 5. Build everything (models + tests + snapshots)
```bash
dbt build
```

### 6. Generate and view documentation
```bash
dbt docs generate
dbt docs serve
```

---

## Author

Adithya Varambally  
Master’s in Data Science, AI, and Digital Business  
Gisma University, Potsdam

---

## License

This project is licensed under the MIT License.  
Feel free to use, modify, and distribute with attribution.

---
