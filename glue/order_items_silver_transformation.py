import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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
AmazonS3_node1790274665401 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://business-insights-project/bronze/order_items/"], "recurse": True}, transformation_ctx="AmazonS3_node1790274665401")

# Script generated for node Change Schema
ChangeSchema_node1790274715433 = ApplyMapping.apply(frame=AmazonS3_node1790274665401, mappings=[("app_name", "string", "app_name", "string"), ("restaurant_id", "string", "restaurant_id", "string"), ("creation_time_utc", "string", "creation_time_utc", "timestamp"), ("order_id", "string", "order_id", "string"), ("user_id", "string", "user_id", "string"), ("printed_card_number", "bigint", "printed_card_number", "string"), ("is_loyalty", "string", "is_loyalty", "boolean"), ("currency", "string", "currency", "string"), ("lineitem_id", "string", "lineitem_id", "string"), ("item_category", "string", "item_category", "string"), ("item_name", "string", "item_name", "string"), ("item_price", "float", "item_price", "decimal"), ("item_quantity", "int", "item_quantity", "int")], transformation_ctx="ChangeSchema_node1790274715433")

# Script generated for node add silver_processed_at column
SqlQuery0 = '''
SELECT
    *,
    current_timestamp() AS silver_processed_at
FROM myDataSource
'''
addsilver_processed_atcolumn_node1790703709627 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":ChangeSchema_node1790274715433}, transformation_ctx = "addsilver_processed_atcolumn_node1790703709627")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=addsilver_processed_atcolumn_node1790703709627, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790274632637", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790275410818 = glueContext.getSink(path="s3://business-insights-project/silver/order_items/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790275410818")
AmazonS3_node1790275410818.setCatalogInfo(catalogDatabase="business_insights_silver_db",catalogTableName="order_items_transformed")
AmazonS3_node1790275410818.setFormat("glueparquet", compression="snappy")
AmazonS3_node1790275410818.writeFrame(addsilver_processed_atcolumn_node1790703709627)
job.commit()