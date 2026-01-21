import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1767987398266 = glueContext.create_dynamic_frame.from_catalog(database="s3-to-glue", table_name="trip_level", transformation_ctx="AWSGlueDataCatalog_node1767987398266")

# Script generated for node Change Schema
ChangeSchema_node1767990212320 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1767987398266, mappings=[("pickup_hour", "int", "pickup_hour", "int"), ("pu_locationid", "int", "pu_locationid", "int"), ("pu_service_zone", "string", "pu_service_zone", "string"), ("tpep_dropoff_datetime", "timestamp", "tpep_dropoff_datetime", "timestamp"), ("congestion_surcharge", "double", "congestion_surcharge", "double"), ("pickup_month", "string", "pickup_month", "string"), ("passenger_count", "double", "passenger_count", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("data_owner", "string", "data_owner", "string"), ("is_long_trip", "int", "is_long_trip", "byte"), ("trip_distance", "double", "trip_distance", "double"), ("cbd_congestion_fee", "double", "cbd_congestion_fee", "double"), ("trip_duration_minutes", "decimal", "trip_duration_minutes", "decimal"), ("do_service_zone", "string", "do_service_zone", "string"), ("ratecodeid", "int", "ratecodeid", "int"), ("extra", "double", "extra", "double"), ("pickup_day", "string", "pickup_day", "string"), ("high_tip_flag", "int", "high_tip_flag", "byte"), ("fare_amount", "double", "fare_amount", "double"), ("vendorid", "int", "vendorid", "int"), ("pickup_day_of_week", "int", "pickup_day_of_week", "int"), ("vendor_name", "string", "vendor_name", "string"), ("tip_pct", "double", "tip_pct", "double"), ("rate_code_desc", "string", "rate_code_desc", "string"), ("do_borough", "string", "do_borough", "string"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("do_zone", "string", "do_zone", "string"), ("pu_borough", "string", "pu_borough", "string"), ("pickup_year", "string", "pickup_year", "string"), ("dolocationid", "int", "dolocationid", "int"), ("pickup_day_of_month", "int", "pickup_day_of_month", "int"), ("pulocationid", "int", "pulocationid", "int"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("payment_type", "int", "payment_type", "int"), ("pu_zone", "string", "pu_zone", "string"), ("rate_code_id", "long", "rate_code_id", "long"), ("total_amount", "double", "total_amount", "double"), ("tip_amount", "double", "tip_amount", "double"), ("mta_tax", "double", "mta_tax", "double"), ("data_zone", "string", "data_zone", "string"), ("airport_fee", "double", "airport_fee", "double"), ("is_short_trip", "int", "is_short_trip", "byte"), ("tpep_pickup_datetime", "timestamp", "tpep_pickup_datetime", "timestamp"), ("do_locationid", "int", "do_locationid", "int")], transformation_ctx="ChangeSchema_node1767990212320")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1767990224332 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1767990212320, database="data_migrate_rds", table_name="nyc_taxi_mdm_mdm_golden_trip_level", transformation_ctx="AWSGlueDataCatalog_node1767990224332")

job.commit()