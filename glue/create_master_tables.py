from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

master_root = "s3://nyc-taxi-lake-0afffdf0f7eb/master/"

# Taxi zones from CSV
zones = spark.read.option("header", True).option("inferSchema", True) \
    .csv("s3://nyc-taxi-lake-0afffdf0f7eb/landing/yellow/taxi_zone_lookup.csv")
zones.write.mode("overwrite").parquet(master_root + "taxi_zone/")

# Vendors from constants
vendors = spark.createDataFrame([
    (1, "Creative Mobile Technologies, LLC"),
    (2, "Curb Mobility, LLC"),
    (6, "Myle Technologies Inc"),
    (7, "Helix"),
], ["vendor_id", "vendor_name"])
vendors.write.mode("overwrite").parquet(master_root + "vendor/")

# Rate codes from constants
rate_codes = spark.createDataFrame([
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride"),
    (99, "Null/unknown"),
], ["rate_code_id", "rate_code_desc"])
rate_codes.write.mode("overwrite").parquet(master_root + "rate_code/")
