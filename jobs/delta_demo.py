# ============================================================
# Delta Lake Demo
# ============================================================
# This script demonstrates how the NYC Taxi pipeline would
# operate using Delta Lake format in a production environment.
#
# WHY DELTA LAKE:
#   - ACID transactions: no partial writes if pipeline crashes
#   - Time travel: query data as it was at any previous version
#   - Schema enforcement: rejects bad writes before they land
#   - Upserts: merge new data without full rewrites
#
# REQUIREMENTS:
#   - Databricks Runtime OR
#   - GCP Dataproc with Delta Lake JAR
#   - pip install delta-spark
#
# NOT RUN LOCALLY: Windows JAR compatibility issues with
# delta-spark + PySpark 3.5.x on local machines.
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# On Databricks, SparkSession is pre-configured with Delta support
# Locally you would add:
# .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
# .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
# .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


CLEANED_PARQUET_PATH = "output/cleaned/"
DELTA_OUTPUT_PATH = "output/delta/cleaned/"


def demo_delta_write(spark):
    """
    Writes cleaned Parquet data as Delta format.
    Delta adds a transaction log (_delta_log/) alongside the data files.
    Every write is versioned — version 0 is the first write.
    """
    df = spark.read.parquet(CLEANED_PARQUET_PATH)

    # Write as Delta instead of Parquet — identical API, different format
    df.write \
        .format("delta") \
        .partitionBy("pickup_year", "pickup_month") \
        .mode("overwrite") \
        .save(DELTA_OUTPUT_PATH)

    print("Delta write complete. Transaction log created at output/delta/cleaned/_delta_log/")


def demo_time_travel(spark):
    """
    Reads Delta table as it was at a specific version.
    Useful for auditing, debugging, or recovering from bad writes.
    
    Example scenario:
        Version 0 — initial load (Jan-Mar 2016 + Jan 2015)
        Version 1 — someone accidentally overwrites with bad data
        Version 0 — you can still read the good data
    """

    # Read current version
    df_current = spark.read \
        .format("delta") \
        .load(DELTA_OUTPUT_PATH)

    # Read as it was at version 0
    df_v0 = spark.read \
        .format("delta") \
        .option("versionAsOf", 0) \
        .load(DELTA_OUTPUT_PATH)

    # Read as it was at a specific timestamp
    df_yesterday = spark.read \
        .format("delta") \
        .option("timestampAsOf", "2026-01-01") \
        .load(DELTA_OUTPUT_PATH)

    print(f"Current row count: {df_current.count()}")
    print(f"Version 0 row count: {df_v0.count()}")


def demo_upsert(spark):
    """
    Merges new incoming taxi data into existing Delta table.
    
    Real world scenario:
        Every day NYC releases new trip data.
        Instead of rewriting the entire dataset, merge only new/changed rows.
        Match on VendorID + pickup datetime as the unique key.
    """
    from delta.tables import DeltaTable

    # Load existing Delta table
    delta_table = DeltaTable.forPath(spark, DELTA_OUTPUT_PATH)

    # Simulate new incoming data (e.g. today's trips)
    new_data = spark.read.parquet("data/raw/new_batch/")

    # Merge — update if exists, insert if new
    delta_table.alias("existing") \
        .merge(
            new_data.alias("incoming"),
            "existing.VendorID = incoming.VendorID AND \
             existing.tpep_pickup_datetime = incoming.tpep_pickup_datetime"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    print("Upsert complete. Only new/changed rows were written.")


if __name__ == "__main__":
    print("Delta Lake Demo — not executed locally")
    print("Deploy to Databricks or GCP Dataproc to run")
    print("See README for deployment instructions")