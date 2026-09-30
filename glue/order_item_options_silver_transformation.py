import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1790317274656 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://business-insights-project/bronze/order_item_options/"], "recurse": True}, transformation_ctx="AmazonS3_node1790317274656")

# Script generated for node Drop Duplicates
DropDuplicates_node1790269999341 =  DynamicFrame.fromDF(AmazonS3_node1790317274656.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1790269999341")

# Script generated for node add silver_processed_at column
SqlQuery0 = '''
SELECT
    *,
    current_timestamp() AS silver_processed_at
FROM myDataSource
'''
addsilver_processed_atcolumn_node1790704320725 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":DropDuplicates_node1790269999341}, transformation_ctx = "addsilver_processed_atcolumn_node1790704320725")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=addsilver_processed_atcolumn_node1790704320725, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790266690207", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790270048574 = glueContext.getSink(path="s3://business-insights-project/silver/order_item_options/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790270048574")
AmazonS3_node1790270048574.setCatalogInfo(catalogDatabase="business_insights_silver_db",catalogTableName="order_item_options_transformed")
AmazonS3_node1790270048574.setFormat("glueparquet", compression="snappy")
AmazonS3_node1790270048574.writeFrame(addsilver_processed_atcolumn_node1790704320725)
job.commit()