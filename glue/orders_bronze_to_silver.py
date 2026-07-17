from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit
)
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    TimestampType
)

import sys


# ----------------------------------------------------------
# Job Arguments
# ----------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "YEAR",
        "MONTH",
        "DAY"
    ]
)

year = args["YEAR"]
month = args["MONTH"]
day = args["DAY"]


# ----------------------------------------------------------
# Job Initialization
# ----------------------------------------------------------

sc = SparkContext()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# ----------------------------------------------------------
# Dynamic Partition Overwrite
# ----------------------------------------------------------

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)


# ----------------------------------------------------------
# Build Incremental Bronze Path
# ----------------------------------------------------------

bronze_path = (
    f"s3://retail-analytics-platform/"
    f"bronze/orders/"
    f"year={year}/"
    f"month={month}/"
    f"day={day}/"
)


# ----------------------------------------------------------
# Read Target Bronze Partition
# ----------------------------------------------------------

df = (
    spark.read
    .option(
        "header",
        "true"
    )
    .csv(
        bronze_path
    )
)


# ----------------------------------------------------------
# Add Partition Columns
# ----------------------------------------------------------

df = (
    df
    .withColumn(
        "year",
        lit(year)
    )
    .withColumn(
        "month",
        lit(month)
    )
    .withColumn(
        "day",
        lit(day)
    )
)


# ----------------------------------------------------------
# Clean and Standardize Orders
# ----------------------------------------------------------

df_cleaned = (
    df
    .dropDuplicates(
        [
            "order_id"
        ]
    )
    .withColumn(
        "quantity",
        col("quantity").cast(
            IntegerType()
        )
    )
    .withColumn(
        "unit_price",
        col("unit_price").cast(
            DoubleType()
        )
    )
    .withColumn(
        "total_amount",
        col("total_amount").cast(
            DoubleType()
        )
    )
    .withColumn(
        "order_timestamp",
        col("order_timestamp").cast(
            TimestampType()
        )
    )
    .withColumn(
        "processed_timestamp",
        current_timestamp()
    )
)


# ----------------------------------------------------------
# Write Target Silver Partition
# ----------------------------------------------------------

(
    df_cleaned.write

    .mode(
        "overwrite"
    )

    .partitionBy(
        "year",
        "month",
        "day"
    )

    .parquet(
        "s3://retail-analytics-platform/"
        "silver/orders/"
    )
)


# ----------------------------------------------------------
# Commit Job
# ----------------------------------------------------------

job.commit()
