import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, when, hour, dayofweek, dayofmonth, year, month, to_timestamp

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


BRONZE_PATH = "s3://nyc-taxi-lake-0afffdf0f7eb/landing/yellow/"  
SILVER_PATH = "s3://nyc-taxi-lake-0afffdf0f7eb/validated/yellow/"  


print(f"📥 Reading Bronze CSV files from: {BRONZE_PATH}")

bronze_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")  # Handles multiple CSV files
    .load(BRONZE_PATH)
)

print(f"Bronze Count: {bronze_df.count()}")
bronze_df.printSchema()


print("🧽 Cleaning data and casting columns...")


numeric_cols = ["passenger_count", "trip_distance", "fare_amount", "total_amount", "tip_amount"]
for col_name in numeric_cols:
    if col_name in bronze_df.columns:
        bronze_df = bronze_df.withColumn(col_name, col(col_name).cast("double"))


silver_df = bronze_df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("total_amount") > 0) &
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull() &
    col("VendorID").isNotNull() &           # Add MDM key validation
    col("RatecodeID").isNotNull() &         # Add MDM key validation
    col("PULocationID").isNotNull() &       # Add MDM key validation
    col("DOLocationID").isNotNull()         # Add MDM key validation
)

print(f"After Cleaning: {silver_df.count()} records")


print("✨ Adding enriched features...")


silver_df = silver_df.withColumn(
    "tpep_pickup_datetime",
    to_timestamp(col("tpep_pickup_datetime"))
)

silver_df = silver_df.withColumn(
    "tpep_dropoff_datetime",
    to_timestamp(col("tpep_dropoff_datetime"))
)


silver_df = silver_df.withColumn(
    "trip_duration_minutes",
    expr("(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0")
)

silver_df = silver_df.withColumn(
    "tip_pct",
    when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100).otherwise(0.0)
)

silver_df = silver_df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_day_of_month", dayofmonth(col("tpep_pickup_datetime")))


silver_df = silver_df.withColumn("is_long_trip", when(col("trip_distance") > 20, 1).otherwise(0))
silver_df = silver_df.withColumn("is_short_trip", when(col("trip_distance") < 1, 1).otherwise(0))
silver_df = silver_df.withColumn("high_tip_flag", when(col("tip_pct") > 20, 1).otherwise(0))


silver_df = silver_df.withColumn("pickup_year", year(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_month", month(col("tpep_pickup_datetime")))
silver_df = silver_df.withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))

print("Schema After Enrichment:")
silver_df.printSchema()


print(f"💾 Writing Silver dataset to: {SILVER_PATH}")

(
    silver_df.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month", "pickup_day")  # Better partitioning than just pickup_date
    .save(SILVER_PATH)
)

print("✅ Validated zone complete!")
