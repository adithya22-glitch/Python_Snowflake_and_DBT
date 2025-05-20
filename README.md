# ESG and Risk Indicators Analysis with Snowflake


### Description
This repository contains code and workflows for extracting, transforming, uploading, and analyzing ESG (Environmental, Social, Governance) and Risk-related financial indicators from the World Bank API. It also integrates external GDP and population data to enable richer insights and visualization.

---
### Technologies Used
- **Python**: Data ingestion, transformation, and automation
- **Snowflake (Snowpark)**: Cloud data warehouse and SQL processing
- **Power BI**: Dashboard creation and visualization
- **Pandas, Matplotlib, Seaborn**: Exploratory data analysis
- **Keyring**: Secure credential management
- **Jupyter Notebook**: Interactive development environment

---
## Contents
- `fetch_data.ipynb`: Extracts ESG data from the World Bank API
- `augment_with_gdp_population.py`: Augments ESG data with GDP and population values
- `upload_to_snowflake.ipynb`: Uploads cleaned data to Snowflake and performs analysis
- `beeper.py`: Optional notification utility

---

### 1. Upload to Snowflake
```python
import pandas as pd
from snowflake.snowpark import Session

df = pd.read_csv("esg_risk_data.csv")

connection_parameters = {
    "account": "<ACCOUNT>",
    "user": "<USER>",
    "password": "<PASSWORD>",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "BANK_DATA",
    "schema": "RAW_DATA"
}

session = Session.builder.configs(connection_parameters).create()
session.write_pandas(df, table_name="BANK_ESG_RISK", auto_create_table=True, overwrite=True)
```
**Purpose:** Uploads the collected ESG dataset to Snowflake in table `RAW_DATA.BANK_ESG_RISK`.

---

### 2. Create Views for Cleaned Data
```python
create_view_sql = """
create or replace view RAW_DATA.stg_esg_risk as
select
  "country",
  "country_name",
  "indicator",
  "indicator_name",
  try_cast("year" as int) as year,
  try_cast("value" as float) as value
from RAW_DATA.BANK_ESG_RISK
where "value" is not null
"""
session.sql(create_view_sql).collect()
```
**Purpose:** Creates a clean view (`stg_esg_risk`) for querying structured ESG values.

```python
create_fact_view_sql = """
create or replace view RAW_DATA.fct_esg_summary as
select
  "country",
  YEAR as year,
  "indicator_name",
  avg(VALUE) as avg_value
from RAW_DATA.stg_esg_risk
where VALUE is not null
group by "country", YEAR, "indicator_name"
"""
session.sql(create_fact_view_sql).collect()
```
**Purpose:** Creates a fact view summarizing ESG values by country and year.

---

### 3. Data Insights and Analysis
**Record Count by Country**
```python
query = """
SELECT "country", "country_name", COUNT(*) as record_count
FROM RAW_DATA.BANK_ESG_RISK
GROUP BY "country", "country_name"
ORDER BY record_count DESC
LIMIT 1000
"""
df = session.sql(query).to_pandas()
```
**Purpose:** Identifies which countries have the most ESG records.

**Statistical Overview**
```python
query = """
SELECT
    "country",
    "country_name",
    COUNT(DISTINCT "indicator") AS num_indicators,
    COUNT(DISTINCT "year") AS num_years,
    AVG("value") AS avg_value,
    MIN("value") AS min_value,
    MAX("value") AS max_value
FROM RAW_DATA.BANK_ESG_RISK
GROUP BY "country", "country_name"
ORDER BY num_indicators DESC, avg_value DESC
"""
df_all_countries = session.sql(query).to_pandas()
```
**Purpose:** Summarizes ESG coverage and performance per country.

---

### 4. Uploading Enriched ESG + GDP + Population Data
```python
import pandas as pd
from snowflake.snowpark import Session

df = pd.read_csv("esg_risk_enriched.csv")
session.write_pandas(df, table_name="BANK_ESG_RISK", auto_create_table=True, overwrite=True)
```
**Purpose:** Replaces original data with augmented ESG + GDP + population.

---

### 5. Analysis Queries
**Top Average Returns by Indicator**
```python
query = """
SELECT "country_name", "indicator_name", AVG("value") AS avg_value
FROM RAW_DATA.BANK_ESG_RISK
WHERE "indicator_name" IN ('Return on Assets (ROA) (%)', 'Return on Equity (ROE) (%)')
GROUP BY "country_name", "indicator_name"
ORDER BY avg_value DESC
LIMIT 20
"""
df_top_returns = session.sql(query).to_pandas()
```

**ESG vs GDP (Top 50)**
```python
query = """
SELECT
    "country_name",
    "indicator_name",
    AVG("value") AS avg_esg,
    AVG("gdp") AS avg_gdp
FROM RAW_DATA.BANK_ESG_RISK
WHERE "gdp" IS NOT NULL AND "value" IS NOT NULL
GROUP BY "country_name", "indicator_name"
ORDER BY avg_gdp DESC
LIMIT 50
"""
df_gdp_esg = session.sql(query).to_pandas()
```

**Top Countries by ESG**
```python
top_esg = df.groupby("country_name")["value"].mean().reset_index(name="avg_esg")
top_esg = top_esg.sort_values("avg_esg", ascending=False).head(10)
```

**Indicator Volatility**
```python
volatility = df.groupby("indicator_name")["value"].std().reset_index(name="volatility")
volatility = volatility.sort_values("volatility", ascending=False)
```

**ESG-GDP Correlation**
```python
correlations = df[["indicator_name", "value", "gdp"]].dropna()
corr_result = correlations.groupby("indicator_name").apply(
    lambda x: x["value"].corr(x["gdp"])
).reset_index(name="gdp_esg_correlation")
corr_result = corr_result.sort_values("gdp_esg_correlation", key=abs, ascending=False)
```

**Improvement Over Time**
```python
improvement = df.dropna(subset=["value", "year"]).copy()
improvement["year"] = improvement["year"].astype(int)
first = improvement.sort_values("year").groupby(["country_name", "indicator_name"]).first().reset_index()
last = improvement.sort_values("year").groupby(["country_name", "indicator_name"]).last().reset_index()

change = pd.merge(last, first, on=["country_name", "indicator_name"], suffixes=("_last", "_first"))
change["improvement"] = change["value_last"] - change["value_first"]
top_improvements = change.sort_values("improvement", ascending=False).head(10)
```

**ESG Per Capita**
```python
esg_per_capita = df.groupby("country_name").apply(
    lambda x: x["value"].sum() / x["population"].mean()
).reset_index(name="esg_per_capita")
esg_per_capita = esg_per_capita.sort_values("esg_per_capita", ascending=False).head(10)
```

---

### Security Note
Do not hardcode credentials in production. Use environment variables or a secrets manager.

---

### Next Steps
- Build a Power BI dashboard for visualization
- Automate data refresh and upload
- Monitor ESG trends over time

---

### Power BI Integration via Python (Indirect Method)
To visualize ESG insights in Power BI, we prepared CSV outputs derived from Snowflake. This allows users to either:
1. Import exported CSVs into Power BI Desktop (Offline Mode), or
2. Use Power BI’s built-in Snowflake connector for a live cloud connection.

The current implementation uses **Option 1**: exporting analytical DataFrames to `.csv` via Snowflake's Python API (Snowpark), making the process reproducible and automatable.

**Secure Connection Using keyring**
```python
keyring.get_password("snowflake", "ADITHYA")
```
This is more secure than storing plaintext passwords in scripts or `.env` files.

**Exported Files for Power BI**
The following datasets are exported into the `powerbi_exports/` directory:

| File                        | Description                                                |
|-----------------------------|------------------------------------------------------------|
| `esg_summary_export.csv`   | Aggregated ESG values by country, year, and indicator      |
| `stg_esg_risk_export.csv`  | Cleaned raw ESG records for flexible querying              |
| `bank_esg_risk_export.csv` | Full ESG dataset including GDP & population               |
| `top_avg_esg.csv`          | Top 10 countries with highest average ESG scores          |
| `indicator_volatility.csv` | Standard deviation per indicator (volatility)             |
| `gdp_esg_correlation.csv`  | Correlation of each ESG indicator with GDP                |
| `top_improvements.csv`     | Top ESG improvements across years by country/indicator    |
| `esg_per_capita.csv`       | ESG values normalized by population per country           |

**Power BI Dashboard Design Suggestions**
After loading the exported CSVs into Power BI, you can visualize insights through:

- **Line Chart**
  - Axis: year
  - Value: value
  - Legend: indicator_name
  - Filters: country_name
  - Use: Track ESG indicator trends over time

- **Bar Chart**
  - Axis: country_name
  - Value: avg_esg
  - Use: Visualize highest scoring ESG countries

- **Matrix / Heatmap**
  - Axis: indicator_name
  - Value: gdp_esg_correlation
  - Use: Highlight which ESG indicators have strong GDP associations

- **Column Chart**
  - Axis: country_name
  - Value: improvement
  - Use: Identify nations showing biggest positive ESG changes

---

### Automation Note
You can schedule this Python export workflow with `cron` (Linux/macOS) or **Windows Task Scheduler** to refresh CSVs daily/weekly, ensuring Power BI reports stay current.

---

### Dashboard Snapshot
![image](https://github.com/user-attachments/assets/ac006a63-7ee8-42bd-b259-46c006500a8d)


---
### Author
**Adithya Varambally** | Data Science | Berlin

---

### License
MIT License

---



