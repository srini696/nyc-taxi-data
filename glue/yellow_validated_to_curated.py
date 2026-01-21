import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as sum_ , avg, when, lit, coalesce, input_file_name, regexp_extract,
    hour, dayofweek, dayofmonth, year, month, to_timestamp
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("validated_to_curated_glue")

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("🚀 Glue Validated → Curated (Enrichment + Aggregations) Job Started")


VALIDATED_PATH = "s3://nyc-taxi-lake-0afffdf0f7eb/validated/yellow/"
CURATED_PATH   = "s3://nyc-taxi-lake-0afffdf0f7eb/curated/yellow_trips_enriched/"
MASTER_ROOT    = "s3://nyc-taxi-lake-0afffdf0f7eb/master/"

validated_df = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .load(VALIDATED_PATH)
)

logger.info(f"Validated records: {validated_df.count()}")


validated_df = validated_df.withColumn(
    "pickup_year",
    coalesce(regexp_extract(input_file_name(), r"pickup_year=(\d+)", 1).cast("int"), year(col("tpep_pickup_datetime")))
)
validated_df = validated_df.withColumn(
    "pickup_month",
    coalesce(regexp_extract(input_file_name(), r"pickup_month=(\d+)", 1).cast("int"), month(col("tpep_pickup_datetime")))
)
validated_df = validated_df.withColumn(
    "pickup_day",
    coalesce(regexp_extract(input_file_name(), r"pickup_day=(\d+)", 1).cast("int"), dayofmonth(col("tpep_pickup_datetime")))
)

validated_df = validated_df.withColumn("VendorID", col("VendorID").cast("int")) \
                          .withColumn("RatecodeID", col("RatecodeID").cast("int")) \
                          .withColumn("PULocationID", col("PULocationID").cast("int")) \
                          .withColumn("DOLocationID", col("DOLocationID").cast("int"))


logger.info("📚 Loading Master Data...")
zones_df = spark.read.format("parquet").load(MASTER_ROOT + "taxi_zone/")
vendors_df = spark.read.format("parquet").load(MASTER_ROOT + "vendor/")
rate_codes_df = spark.read.format("parquet").load(MASTER_ROOT + "rate_code/")


logger.info("🔗 Creating Trip-Level Golden Records...")


enriched_trips = validated_df.join(
    vendors_df.select("vendor_id", "vendor_name"),
    validated_df.VendorID == vendors_df.vendor_id, "left"
).join(
    rate_codes_df.select("rate_code_id", "rate_code_desc"),
    validated_df.RatecodeID == rate_codes_df.rate_code_id, "left"
)


pickup_zones = zones_df.select(
    col("LocationID").alias("PU_LocationID"),
    col("Borough").alias("PU_Borough"),
    col("Zone").alias("PU_Zone"),
    col("service_zone").alias("PU_service_zone")
)
dropoff_zones = zones_df.select(
    col("LocationID").alias("DO_LocationID"),
    col("Borough").alias("DO_Borough"),
    col("Zone").alias("DO_Zone"),
    col("service_zone").alias("DO_service_zone")
)

enriched_trips = enriched_trips.join(pickup_zones, enriched_trips.PULocationID == pickup_zones.PU_LocationID, "left") \
                              .join(dropoff_zones, enriched_trips.DOLocationID == dropoff_zones.DO_LocationID, "left")


total_records = enriched_trips.count()
golden_count = enriched_trips.filter(
    col("vendor_name").isNotNull() & 
    col("rate_code_desc").isNotNull() &
    col("PU_Zone").isNotNull() &
    col("DO_Zone").isNotNull()
).count()

if golden_count / total_records < 0.99:
    logger.error("❌ Master data quality threshold not met")
    raise Exception("Governance rule violation")


golden_trips = enriched_trips.filter(
    col("vendor_name").isNotNull() & 
    col("rate_code_desc").isNotNull() &
    col("PU_Zone").isNotNull() &
    col("DO_Zone").isNotNull()
).withColumn("data_zone", lit("curated_trip_level")) \
 .withColumn("data_owner", lit("NYC Ops VP"))


logger.info("📊 Creating Gold Aggregations...")


aggregated_input = golden_trips.withColumn(
    "rush_hour_flag",
    when(col("pickup_hour").between(7, 10), 1)
    .when(col("pickup_hour").between(16, 19), 1)
    .otherwise(0)
)


gold_df = aggregated_input.groupBy(
    "pickup_year", "pickup_month", "pickup_day",
    "pickup_hour", "pickup_day_of_week", "pickup_day_of_month",
    "PULocationID", "PU_Zone", "PU_Borough",    # Include zone names
    "DOLocationID", "DO_Zone", "DO_Borough"    # Include zone names
).agg(
    count("*").alias("total_rides"),
    sum_("fare_amount").alias("total_fare"),
    sum_("tip_amount").alias("total_tip"),
    sum_("total_amount").alias("total_revenue"),
    avg("fare_amount").alias("avg_fare"),
    avg("tip_pct").alias("avg_tip_pct"),
    sum_("high_tip_flag").alias("num_high_tip_trips"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("trip_duration_minutes").alias("avg_trip_duration"),
    sum_("trip_distance").alias("sum_trip_distance"),
    sum_("is_long_trip").alias("num_long_trips"),
    sum_("is_short_trip").alias("num_short_trips"),
    sum_("rush_hour_flag").alias("num_rush_hour_rides")
)


logger.info("📈 Adding derived KPIs...")

gold_df = gold_df.withColumn(
    "revenue_per_mile",
    when(col("sum_trip_distance") > 0,
         col("total_revenue") / col("sum_trip_distance"))
    .otherwise(lit(0.0))
).withColumn(
    "avg_revenue_per_ride",
    col("total_revenue") / col("total_rides")
).withColumn(
    "avg_speed_mph",
    when(col("avg_trip_duration") > 0,
         col("avg_trip_distance") / (col("avg_trip_duration") / 60))
    .otherwise(lit(0.0))
).withColumn(
    "tip_per_mile",
    when(col("sum_trip_distance") > 0,
         col("total_tip") / col("sum_trip_distance"))
    .otherwise(lit(0.0))
)


gold_df = gold_df.withColumn("data_zone", lit("curated_aggregated")) \
                 .withColumn("data_owner", lit("NYC Ops VP")) \
                 .withColumn("data_domain", lit("nyc_taxi_kpis")) \
                 .withColumn("aggregated_at", lit(spark.sparkContext.startTime))


gold_df = gold_df.withColumn("pickup_year", coalesce(col("pickup_year"), lit(0))) \
                 .withColumn("pickup_month", coalesce(col("pickup_month"), lit(0))) \
                 .withColumn("pickup_day", coalesce(col("pickup_day"), lit(0)))


logger.info(f"💾 Writing Trip-Level Golden Records...")
golden_trips.write.format("parquet").mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month", "pickup_day") \
    .save(CURATED_PATH + "trip_level/")

logger.info(f"💾 Writing Aggregated KPIs...")
gold_df.write.format("parquet").mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month", "pickup_day") \
    .save(CURATED_PATH + "kpis/")

logger.info("✅ Curated zone complete with both trip-level and aggregated data!")
job.commit()
logger.info("🏁 Validated → Curated ETL Job Finished Successfully")
