import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import boto3
import json
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node date_dim - sql - source
date_dimsqlsource_node1790223940834 = glueContext.create_dynamic_frame.from_options(
    connection_type = "sqlserver",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "date_dim",
        "connectionName": "sqlserver_jdbc",
    },
    transformation_ctx = "date_dimsqlsource_node1790223940834"
)


# Script generated for node order_items - sql - source
order_itemssqlsource_node1790223990180 = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "order_items",
        "connectionName": "sqlserver_jdbc",
    },
    transformation_ctx="order_itemssqlsource_node1790223990180"
)

# Script generated for node order_item_options - sql - source
order_item_optionssqlsource_node1790225461936 = glueContext.create_dynamic_frame.from_options(
    connection_type="sqlserver",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "order_item_options",
        "connectionName": "sqlserver_jdbc"
    },
    transformation_ctx="order_item_optionssqlsource_node1790225461936"
)

s3 = boto3.client("s3")
watermark_bucket = "business-insights-project"

# order_items watermark
order_items_watermark_key = "watermarks/order_items.json"

response = s3.get_object(
    Bucket=watermark_bucket,
    Key=order_items_watermark_key
)

order_items_watermark_data = json.loads(
    response["Body"].read().decode("utf-8")
)

order_items_last_watermark = order_items_watermark_data["last_watermark"]

order_items_df = order_itemssqlsource_node1790223990180.toDF()

order_items_incremental_df = order_items_df.filter(
    order_items_df["creation_time_utc"] > order_items_last_watermark
)

order_items_incremental = DynamicFrame.fromDF(
    order_items_incremental_df,
    glueContext,
    "order_items_incremental"
)

# order_item_options watermark
order_item_options_watermark_key = "watermarks/order_item_options.json"

response = s3.get_object(
    Bucket=watermark_bucket,
    Key=order_item_options_watermark_key
)

order_item_options_watermark_data = json.loads(
    response["Body"].read().decode("utf-8")
)

order_item_options_last_watermark = order_item_options_watermark_data["last_watermark"]

order_item_options_df = order_item_optionssqlsource_node1790225461936.toDF()

order_item_options_incremental_df = order_item_options_df.filter(
    order_item_options_df["creation_time_utc"] > order_item_options_last_watermark
)

order_item_options_incremental = DynamicFrame.fromDF(
    order_item_options_incremental_df,
    glueContext,
    "order_item_options_incremental"
)

# Full refresh for date_dim - delete existing Bronze files
date_dim_bucket = "business-insights-project"
date_dim_prefix = "bronze/date_dim/"

existing_objects = s3.list_objects_v2(
    Bucket=date_dim_bucket,
    Prefix=date_dim_prefix
)

if "Contents" in existing_objects:
    s3.delete_objects(
        Bucket=date_dim_bucket,
        Delete={
            "Objects": [
                {"Key": obj["Key"]}
                for obj in existing_objects["Contents"]
            ]
        }
    )

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=date_dimsqlsource_node1790223940834, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790223774123", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790224035017 = glueContext.write_dynamic_frame.from_options(frame=date_dimsqlsource_node1790223940834, connection_type="s3", format="glueparquet", connection_options={"path": "s3://business-insights-project/bronze/date_dim/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1790224035017")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=order_items_incremental, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790223774123", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790224749068 = glueContext.write_dynamic_frame.from_options(frame=order_items_incremental, connection_type="s3", format="glueparquet", connection_options={"path": "s3://business-insights-project/bronze/order_items/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1790224749068")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=order_item_options_incremental, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790223774123", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790225543521 = glueContext.write_dynamic_frame.from_options(frame=order_item_options_incremental, connection_type="s3", format="glueparquet", connection_options={"path": "s3://business-insights-project/bronze/order_item_options/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1790225543521")

# Update order_items watermark
if order_items_incremental_df.count() > 0:

    new_watermark = order_items_incremental_df.agg(
        {"creation_time_utc": "max"}
    ).collect()[0][0]

    if new_watermark is not None:
        s3.put_object(
            Bucket=watermark_bucket,
            Key=order_items_watermark_key,
            Body=json.dumps({
                "last_watermark": str(new_watermark)
            })
        )

# Update order_item_options watermark
if order_item_options_incremental_df.count() > 0:

    new_watermark = order_item_options_incremental_df.agg(
        {"creation_time_utc": "max"}
    ).collect()[0][0]

    if new_watermark is not None:
        s3.put_object(
            Bucket=watermark_bucket,
            Key=order_item_options_watermark_key,
            Body=json.dumps({
                "last_watermark": str(new_watermark)
            })
        )

job.commit()