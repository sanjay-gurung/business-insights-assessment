import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node date_key conversion to date type
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import to_date
    from awsglue.dynamicframe import DynamicFrame

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = df.withColumn(
        "date_key",
        to_date("date_key", "dd-MM-yyyy")
    )

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "date_key_converted")

    return DynamicFrameCollection(
        {"CustomTransform": dynamic_frame},
        glueContext
    )
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
AmazonS3_node1790225637736 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://business-insights-project/bronze/date_dim/"], "recurse": True}, transformation_ctx="AmazonS3_node1790225637736")

# Script generated for node Change Schema
ChangeSchema_node1790222892841 = ApplyMapping.apply(frame=AmazonS3_node1790225637736, mappings=[("date_key", "string", "date_key", "string"), ("year", "int", "year", "int"), ("month", "int", "month", "int"), ("week", "int", "week", "int"), ("day_of_week", "string", "day_of_week", "string"), ("is_weekend", "string", "is_weekend", "boolean"), ("is_holiday", "string", "is_holiday", "boolean"), ("holiday_name", "string", "holiday_name", "string")], transformation_ctx="ChangeSchema_node1790222892841")

# Script generated for node date_key conversion to date type
date_keyconversiontodatetype_node1790230637258 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1790222892841": ChangeSchema_node1790222892841}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1790230832861 = SelectFromCollection.apply(dfc=date_keyconversiontodatetype_node1790230637258, key=list(date_keyconversiontodatetype_node1790230637258.keys())[0], transformation_ctx="SelectFromCollection_node1790230832861")

# Script generated for node add silver_processed_at column
SqlQuery0 = '''
SELECT
    *,
    current_timestamp() AS silver_processed_at
FROM myDataSource
'''
addsilver_processed_atcolumn_node1790704838153 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":SelectFromCollection_node1790230832861}, transformation_ctx = "addsilver_processed_atcolumn_node1790704838153")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=addsilver_processed_atcolumn_node1790704838153, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790219708743", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790223522743 = glueContext.getSink(path="s3://business-insights-project/silver/date_dim/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790223522743")
AmazonS3_node1790223522743.setCatalogInfo(catalogDatabase="business_insights_silver_db",catalogTableName="date_dim_transformed")
AmazonS3_node1790223522743.setFormat("glueparquet", compression="snappy")
AmazonS3_node1790223522743.writeFrame(addsilver_processed_atcolumn_node1790704838153)
job.commit()