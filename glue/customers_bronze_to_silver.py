import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    current_timestamp,
    to_date,
    col,
    input_file_name,
    regexp_extract
)

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME"]
)

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# Read Bronze Customers
df = spark.read.option(
    "header",
    True
).csv(
    "s3://retail-analytics-platform/bronze/customers/"
)


# Extract partition columns
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


# Remove duplicate customers
df = df.dropDuplicates(
    ["customer_id"]
)


# Standardize datatypes
df = (
    df
    .withColumn(
        "signup_date",
        to_date(
            col("signup_date")
        )
    )
)


# Add ETL metadata
df = df.withColumn(
    "processed_timestamp",
    current_timestamp()
)


# Remove temporary column
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
    "s3://retail-analytics-platform/silver/customers/"
)


job.commit()
