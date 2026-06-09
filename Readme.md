# Spark Taxi Pipeline

Batch processing pipeline built with PySpark on 46M+ NYC Yellow Taxi trips (2015–2016).
Demonstrates distributed data processing, schema enforcement, domain-driven data cleaning, partitioned Parquet output, and production-grade pipeline patterns.

---

## Project Structure

```
Spark-Taxi-Pipeline/
├── data/
│   └── raw/                      ← source CSVs (not tracked in git)
├── output/
│   ├── cleaned/                  ← partitioned Parquet output
│   ├── aggregations/             ← business metrics
│   │   ├── hourly_demand/
│   │   ├── revenue_by_vendor/
│   │   └── avg_duration_by_hour/
│   ├── quality_report.json       ← data quality report (auto-generated)
│   └── pipeline.log              ← pipeline run logs (auto-generated)
├── jobs/
│   ├── clean.ipynb               ← exploratory cleaning notebook
│   ├── transform.ipynb           ← exploratory transform notebook
│   ├── pipeline.py               ← production pipeline runner
│   └── delta_demo.py             ← Delta Lake production extension (see below)
├── requirements.txt
└── README.md
```

---

## Dataset

**NYC Yellow Taxi Trip Data** — Jan 2015 and Jan–Mar 2016  
Source: [Kaggle](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)  
Size: ~8GB raw CSV, 47.2M rows  
After cleaning: 46M rows (~2.6% removed)

---

## Pipeline

```
data/raw/ (CSV)
    ↓
validate_schema()
    - Checks all 19 columns present with correct types
    - Fails loudly on schema drift before any processing
    ↓
run_cleaning()
    - Schema enforcement (19 columns, typed)
    - Domain-driven filtering (fares, distance, geography, passengers)
    - Partitioned by pickup_year / pickup_month
    - Generates output/quality_report.json
    ↓
output/cleaned/ (Parquet)
    ↓
run_transforms()
    - Hourly demand (trip count by hour of day)
    - Revenue by vendor (total revenue, avg fare, avg tip, trip count)
    - Avg trip duration by hour
    ↓
validate_outputs()
    - Asserts row counts, value ranges, and VendorID validity
    - Fails loudly if aggregations produce unexpected results
    ↓
output/aggregations/ (Parquet)
```

---

## Data Cleaning Rules

| Column | Rule | Reason |
|---|---|---|
| passenger_count | 1–6 | NYC taxi legal capacity |
| fare_amount | $3–$500 | Minimum flag drop to realistic max |
| trip_distance | 0.1–50 miles | NYC geographic bounds |
| pickup/dropoff coords | NYC bounding box | Remove out-of-city GPS errors |
| tip_amount | 0–$100 | Credit card tips only in dataset |
| extra | 0–$1 | Only $0.50 and $1.00 surcharges valid |
| tolls_amount | 0–$40 | NYC geography-constrained max |

---

## Key Findings

- **Peak demand**: 17:00–19:00 (2.4M–2.9M trips/hour)
- **Slowest hour**: 05:00 (~460K trips)
- **Longest avg trip duration**: 22:00 (16.7 min) and 14:00–15:00 (~16.3 min)
- **Vendor 2** handles more volume (24.6M vs 21.4M trips) at similar avg fare
- **Tip rate**: consistent across vendors (~$1.68–$1.71 avg), cash tips not captured

---

## How to Run

**Requirements**
- Python 3.9+
- Java 11
- PySpark 3.5.3

```bash
pip install -r requirements.txt
python jobs/pipeline.py
```

A `output/quality_report.json` and `output/pipeline.log` are generated automatically on every run.

---

## Design Decisions

**Idempotent pipeline** — all writes use `mode("overwrite")`. Running the pipeline multiple times on the same input always produces identical output with no duplicates or side effects.

**Schema validation on ingest** — incoming CSVs are validated against an expected schema before any processing begins. Missing columns or type mismatches raise a descriptive `ValueError` immediately, preventing silent data corruption downstream.

**Output validation** — after every transform job, assertions verify that aggregation outputs have correct row counts, valid value ranges, and expected categorical values. Acts as a lightweight data contract.

**Domain-driven cleaning** — filter bounds are derived from NYC geography and taxi regulations, not statistical heuristics. This makes the cleaning logic explainable and auditable.

**Partitioned reads** — the `run_yearly_analysis()` function demonstrates reading specific year partitions only, triggering Spark's partition pruning to skip irrelevant folders entirely.

**Broadcast join** — vendor lookup (2 rows) is joined to the 46M row dataset using a broadcast hint to avoid shuffle. On memory-constrained local machines a `when/otherwise` expression is used as an equivalent — the broadcast approach is the production pattern.

---

## Delta Lake — Production Extension

`jobs/delta_demo.py` demonstrates how this pipeline would operate with Delta Lake format in production (Databricks / GCP Dataproc).

**Why Delta Lake over plain Parquet:**

| Feature | Parquet | Delta Lake |
|---|---|---|
| ACID transactions | ❌ partial writes possible | ✅ all-or-nothing commits |
| Time travel | ❌ | ✅ query any previous version |
| Schema enforcement on write | ❌ silently accepts bad data | ✅ rejects schema violations |
| Upserts | ❌ full rewrite required | ✅ merge only changed rows |

**Time travel example:**
```python
# Read data as it was at version 0
df = spark.read.format("delta").option("versionAsOf", 0).load("output/delta/cleaned/")
```

**Upsert example (daily incremental loads):**
```python
delta_table.alias("existing") \
    .merge(new_data.alias("incoming"), "existing.VendorID = incoming.VendorID AND ...") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
```

Delta Lake is the default storage format on Databricks and is production-standard across most enterprise Spark deployments.

---

## Requirements.txt

```
pyspark==3.5.3
```

---

## Concepts Demonstrated

- Distributed thinking — why Pandas breaks at 8GB, how Spark partitions work
- Lazy evaluation — transformation plans vs actions
- Shuffle awareness — groupBy and join costs, broadcast joins for small tables
- Partition pruning — year/month folder structure for fast reads
- Domain-driven cleaning — geography, business rules, not just statistics
- Schema validation — catching drift before it corrupts the pipeline
- Output validation — asserting business logic on aggregation results
- Structured logging — timestamped logs to file and terminal on every run
- Idempotency — safe to rerun without side effects
- Delta Lake — ACID transactions, time travel, upserts (production extension)