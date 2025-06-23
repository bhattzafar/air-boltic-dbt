# ✈️ Air Boltic — Analytics Engineer Case Study (dbt Project)
## By Zafrullah Bhatti

Welcome to the official `dbt` project for **Air Boltic**, a case study designed to model, track, and analyze success metrics across a hypothetical airline booking platform operating in Europe.

This project was created to identify **ingest all raw data for airboltic**, implement **base data models to track performance** of key KPIs, and lay the foundation for **strategic analytics and expansion planning**. The project allows analyst to:
	•	Analyze reliable data.
 	•	Access to fact tables and aggregated tables.
  	•	Platform to setup jobs and schedules that allow monitoring and evaluations.
   	•	easily pull up reports that can answer questions for a range of stakeholders from C-level management to internal and external teams.

---

## ⚠️ Note on Raw Data Formatting

When uploading the raw Excel files into Databricks (as Delta tables), some **column names were manually updated**.

> ❗ **Reason:** Databricks Delta tables do **not support column names with spaces** cleanly.  
> To avoid schema conflicts and improve compatibility, column names were normalized to `snake_case`  
> _e.g._ `Customer Name` → `customer_name`.

If you're loading the data from scratch, please ensure column names are adjusted accordingly  
**or include a transformation step in staging models.**

---

## 🚀 Running the Project

> Make sure `dbt` is installed and you're authenticated to Databricks.

### ⏯️ Quickstart

```bash
# Install packages
dbt deps

# Compile and preview
dbt compile

# Run full pipeline
dbt build

# Or run individual steps
dbt seed
dbt run


⸻

⚙️ Project Configuration

dbt_project.yml
	•	Defines model folders and default materializations (table)
	•	Configures quoting:
	•	schema: true enables layered schema creation
	•	Folders are organized as:
	•	01_staging/ → source cleanup
	•	02_intermediate/ → business logic
	•	03_marts/ → final models and dashboard outputs

profiles.yml

Make sure your local ~/.dbt/profiles.yml contains:

air_boltic_dbt:
  outputs:
    dev:
      type: databricks
      ...
  target: dev


⸻

🧠 Project Structure

🔁 macros/

Reusable SQL logic and environment control:
	•	generate_surrogate_key() — for deduplication and joins
	•	is_dev_env() — toggles sample preview (last 90 days only)
	•	Rolling window macros — simplify metric CTEs

⸻

🏗️ models/

✅ 01_staging/

Cleans raw Excel-derived tables:
	•	Normalizes column names
	•	Parses timestamps and formats geography
	•	Remove PII information
    •	Basic schema testing
    

✅ 02_intermediate/

Contains all core transformations and aggregations:
	•	int_market_success — KPIs by destination city
	•	int_airplane_market_success — KPIs by aircraft model & destination
	•	int_customer_segmentation_analysis — metrics by customer group + airplane segment
	•	fact_success_summary — daily snapshots via date spine

✅ 03_marts/

Final outputs, enriched models, and dashboard-ready artifacts:
	•	fact_success_score — weighted success score for each market
	•	Views defined for facts and dimensions to be synced with Looker.

⸻

🧪 tests/

Includes:
	•	Standard dbt tests (unique, not_null, relationships)
	•	dbt-expectations tests:
	•	Value ranges (e.g. revenue ≥ 0)
	•	Ratios in [0, 1] range
	•	Custom tests:
	•	test_metric_is_trending_up — warns if KPIs are dropping over time

All are set to severity: warn so pipeline continues while catching anomalies.

⸻

🌱 seeds/

Why Seeds?

Some models return empty results due to lack of realistic data. Seeds are used for:
	1.	Simulated Output:
Each key model includes a UNION with seed data for visibility in dashboards.
	2.	Snapshot Prototypes:
Future: static lookup tables, thresholds, segment mappings.

dbt seed


⸻

🧭 Models to Know

Model Name	Purpose
int_market_success	Metrics at destination_city level
int_airplane_market_success	Metrics grouped by plane model and destination
int_customer_segmentation_analysis	Customer behavior across booking & aircraft attributes
fact_success_summary	Historical snapshot model for daily metrics
fact_success_score	Weighted success score per market
⸻
