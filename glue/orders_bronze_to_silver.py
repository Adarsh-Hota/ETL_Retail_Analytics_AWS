from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp, input_file_name, regexp_extract
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    TimestampType
)

import sys

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# Read bronze data from Glue Catalog
df = spark.read.option("header", "true").csv(
    "s3://retail-analytics-platform/bronze/orders/"
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

# Basic transformations
df_cleaned = (
    df
    .dropDuplicates(["order_id"])
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    .withColumn("unit_price", col("unit_price").cast(DoubleType()))
    .withColumn("total_amount", col("total_amount").cast(DoubleType()))
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
)


# Write parquet to silver layer
df_cleaned.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(
        "s3://retail-analytics-platform/silver/orders/"
    )


job.commit()
