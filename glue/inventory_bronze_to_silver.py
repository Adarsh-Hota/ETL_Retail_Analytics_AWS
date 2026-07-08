import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    current_timestamp,
    to_timestamp,
    col,
    input_file_name,
    regexp_extract
)

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME']
)

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(args['JOB_NAME'], args)


# Read Bronze Inventory CSV
df = spark.read.option(
    "header",
    True
).csv(
    "s3://retail-analytics-platform/bronze/inventory/"
)

df = (
    df
    .withColumn(
        "input_file",
        input_file_name()
    )
    .withColumn(
        "year",
        regexp_extract(
            col("input_file"),
            r"year=(\d+)",
            1
        )
    )
    .withColumn(
        "month",
        regexp_extract(
            col("input_file"),
            r"month=(\d+)",
            1
        )
    )
    .withColumn(
        "day",
        regexp_extract(
            col("input_file"),
            r"day=(\d+)",
            1
        )
    )
)

df = df.dropDuplicates(
    ["inventory_event_id"]
)

# Standardize datatypes
df = df.withColumn(
    "event_timestamp",
    to_timestamp(
        col("event_timestamp")
    )
)

df = df.withColumn(
    "quantity_change",
    col("quantity_change").cast("int")
)


# Add ETL metadata
df = df.withColumn(
    "processed_timestamp",
    current_timestamp()
)

df = df.drop(
    "input_file"
)

# Write Silver Layer
df.write.mode(
    "append"
).partitionBy(
    "year",
    "month",
    "day"
).parquet(
    "s3://retail-analytics-platform/silver/inventory/"
)


job.commit()
