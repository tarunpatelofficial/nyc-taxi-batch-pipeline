import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import avg, count, sum
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import format_number
from pyspark.sql import functions as F
from pyspark.sql.functions import month, year
from pyspark.sql.functions import unix_timestamp
from datetime import datetime
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("output/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_spark():
    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()

    spark = SparkSession.builder \
        .appName("TaxiPipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    return spark  # build and return SparkSession

def run_cleaning(spark):
    logger.info("Starting cleaning job...")

    schema = StructType([

        StructField("VendorID", IntegerType(),True),
        StructField("tpep_pickup_datetime", TimestampType(),True),
        StructField("tpep_dropoff_datetime", TimestampType(),True),
        StructField("passenger_count", IntegerType(),False),
        StructField("trip_distance", FloatType(),False),
        StructField("pickup_longitude", FloatType(),False),
        StructField("pickup_latitude", FloatType(),False),
        StructField("RateCodeID", IntegerType(),True),
        StructField("store_and_fwd_flag", StringType(),True),
        StructField("dropoff_longitude", FloatType(),True),
        StructField("dropoff_latitude", FloatType(),True),
        StructField("payment_type", IntegerType(),False),
        StructField("fare_amount", FloatType(),True),
        StructField("extra", FloatType(),True),
        StructField("mta_tax", FloatType(),True),
        StructField("tip_amount", FloatType(),True),
        StructField("tolls_amount", FloatType(),True),
        StructField("improvement_surcharge", FloatType(),True),
        StructField("total_amount", FloatType(),False),
    ])
    
    df = spark.read.csv("data/raw/", header=True, schema=schema)

    logger.info("Schema: ")
    df.printSchema()
    
    logger.info(f"Rows read: {df.count()}")
    logger.info(f"Columns: {len(df.columns)}")

    def filter_passengers(df):
        return df.filter(
            col("passenger_count").between(1, 6)
        )


    def filter_fares(df):
        return df.filter(
            col("fare_amount").between(3, 500)
        )


    def filter_distance(df):
        return df.filter(
            col("trip_distance").between(0.1, 50)
        )


    def filter_geography(df):
        return df.filter(
            col("pickup_latitude").between(40.4, 41.2) &
            col("dropoff_latitude").between(40.4, 41.2) &
            col("pickup_longitude").between(-74.3, -73.7) &
            col("dropoff_longitude").between(-74.3, -73.7)
        )


    def filter_tips(df):
        return df.filter(
            col("tip_amount").between(0, 100)
        )


    def filter_surcharges(df):
        return df.filter(
            col("extra").between(0, 1) &
            col("tolls_amount").between(0, 40)
        )
    
    before = df.count()

    df = (
    df.transform(filter_passengers)
      .transform(filter_fares)
      .transform(filter_distance)
      .transform(filter_geography)
      .transform(filter_tips)
      .transform(filter_surcharges)
    )

    after = df.count()

    logger.info(f"Rows before cleaning: {before}")
    logger.info(f"Rows after cleaning:  {after}")
    logger.info(f"Rows removed:         {before - after}")

    df = df.withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_year", year(col("tpep_pickup_datetime")))
    
    df = df.repartition(8)

    df.write \
    .partitionBy("pickup_year", "pickup_month") \
    .mode("overwrite") \
    .parquet("../output/cleaned/")

    logger.info("Saved: output/cleaned/")
    logger.info("Cleaning done.")

    df_verify = spark.read.parquet("../output/cleaned/")

    partition_df  = df_verify.groupBy("pickup_year", "pickup_month") \
            .count() \
            .orderBy("pickup_year", "pickup_month") 
    
    partition_df.show()
    
    partition_counts = {
    f"{row['pickup_year']}-{row['pickup_month']}": row['count']
    for row in partition_df.collect()}

    generate_quality_report(before, after, partition_counts)

def run_transforms(spark):
    logger.info("Starting transforms...")
    df = spark.read.parquet("../output/cleaned/")

    revenue_by_vendor = df.groupBy("VendorID") \
    .agg(
        format_number(sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip"),
        count("*").alias("total_trips")
    )

    
    hourly_demand = (
        df.groupBy(F.hour("tpep_pickup_datetime").alias("pickup_hour"))
        .count()
        .orderBy("pickup_hour")
    )

    hourly_demand.write \
        .mode("overwrite") \
        .parquet("../output/aggregations/hourly_demand/")

    revenue_by_vendor.write \
        .mode("overwrite") \
        .parquet("../output/aggregations/revenue_by_vendor/")
    

    duration_minutes  = df.withColumn("trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime")
        - unix_timestamp("tpep_pickup_datetime")
        ) / 60
        )

    duration_minutes = duration_minutes.filter(col("trip_duration_minutes") > 0)    

    avg_duration_minutes_by_hour = (
    duration_minutes
    .groupBy(F.hour("tpep_pickup_datetime").alias("pickup_hour"))
    .agg(spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"))
    .orderBy("pickup_hour")
    )

    
    avg_duration_minutes_by_hour.write \
    .mode("overwrite") \
    .parquet("../output/aggregations/avg_duration_by_hour/")


    logger.info("Saved: output/aggregations/hourly_demand/")
    logger.info("Saved: output/aggregations/revenue_by_vendor/")
    logger.info("Saved: output/aggregations/avg_duration_by_hour/")

    logger.info("Transforms done.")   

    logger.info("\nRevenue by Vendor")
    revenue_by_vendor.show()

    logger.info("\nHourly Demand")
    hourly_demand.show(24)

    logger.info("\navg duration minutes by hour")
    avg_duration_minutes_by_hour.show(24)

def generate_quality_report(before, after, partition_counts):
    current_time = datetime.now()

    output = {
        "run_timestamp": current_time,
        "raw_row_count": before,
        "cleaned_row_count": after,
        "rows_removed": before - after,
        "removal_percentage": round(((before - after) / before) * 100, 2),
        "rows_per_partition": partition_counts
    }

    with open("output/quality_report.json", "w") as f:
        json.dump(output, f, indent=4, default=str)

    logger.info("Report Saved: output/quality_report.json")

if __name__ == "__main__":
    spark = get_spark()
    run_cleaning(spark)
    run_transforms(spark)
    spark.stop()