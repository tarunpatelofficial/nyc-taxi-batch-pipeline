# Spark Taxi Pipeline

Batch processing pipeline built with PySpark on 46M+ NYC Yellow Taxi trips (2015–2016).
Demonstrates distributed data processing, schema enforcement, domain-driven data cleaning, and partitioned Parquet output.

---

## Project Structure

```
Spark-Taxi-Pipeline/
├── data/
│   └── raw/                    ← source CSVs (not tracked in git)
├── output/
│   ├── cleaned/                ← partitioned Parquet output
│   └── aggregations/           ← business metrics
│       ├── hourly_demand/
│       ├── revenue_by_vendor/
│       └── avg_duration_by_hour/
├── jobs/
│   ├── clean.ipynb             ← exploratory cleaning notebook
│   ├── transform.ipynb         ← exploratory transform notebook
│   └── pipeline.py             ← production pipeline runner
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
run_cleaning()
    - Schema enforcement (19 columns, typed)
    - Domain-driven filtering (fares, distance, geography, passengers)
    - Partitioned by pickup_year / pickup_month
    ↓
output/cleaned/ (Parquet)
    ↓
run_transforms()
    - Hourly demand (trip count by hour of day)
    - Revenue by vendor (total revenue, avg fare, avg tip, trip count)
    - Avg trip duration by hour
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

---

## Requirements.txt

```
pyspark==3.5.3
```

---

## Concepts Demonstrated

- Distributed thinking — why Pandas breaks at 8GB, how Spark partitions work
- Lazy evaluation — transformation plans vs actions
- Shuffle awareness — groupBy and join costs
- Partition pruning — year/month folder structure for fast reads
- Domain-driven cleaning — geography, business rules, not just statistics