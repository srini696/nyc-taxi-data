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
AWSGlueDataCatalog_node1768003323784 = glueContext.create_dynamic_frame.from_catalog(database="nyc_taxi_db", table_name="taxi_zone_lookup_csv", transformation_ctx="AWSGlueDataCatalog_node1768003323784")

# Script generated for node Change Schema
ChangeSchema_node1768003347870 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1768003323784, mappings=[("locationid", "long", "source_zone_id", "int"), ("zone", "string", "zone_name", "string")], transformation_ctx="ChangeSchema_node1768003347870")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1768003350872 = glueContext.write_dynamic_frame.from_catalog(frame=ChangeSchema_node1768003347870, database="data_migrate_rds", table_name="nyc_taxi_mdm_mdm_zone", transformation_ctx="AWSGlueDataCatalog_node1768003350872")

job.commit()