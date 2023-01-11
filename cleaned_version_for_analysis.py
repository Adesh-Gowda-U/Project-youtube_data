import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1672350356457 = glueContext.create_dynamic_frame.from_catalog(
    database="project3_youtube_cleaned",
    table_name="cleaned_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1672350356457",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1672350401776 = glueContext.create_dynamic_frame.from_catalog(
    database="project3_youtube_cleaned",
    table_name="cleaned_statistics_reference_data",
    transformation_ctx="AWSGlueDataCatalog_node1672350401776",
)

# Script generated for node Join
Join_node1672350456891 = Join.apply(
    frame1=AWSGlueDataCatalog_node1672350401776,
    frame2=AWSGlueDataCatalog_node1672350356457,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1672350456891",
)

# Script generated for node Amazon S3
AmazonS3_node1672350803096 = glueContext.getSink(
    path="s3://adesh-project-3-analysis-data",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region", "category_id"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1672350803096",
)
AmazonS3_node1672350803096.setCatalogInfo(
    catalogDatabase="project-3-youtube-analysis",
    catalogTableName="youtube-analysis-data",
)
AmazonS3_node1672350803096.setFormat("glueparquet")
AmazonS3_node1672350803096.writeFrame(Join_node1672350456891)
job.commit()
