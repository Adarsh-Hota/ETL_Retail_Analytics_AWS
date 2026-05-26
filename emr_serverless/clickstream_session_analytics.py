from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    min,
    max,
    count,
    when,
    unix_timestamp,
    to_timestamp,
    col
)

INPUT_PATH = (
    "s3://retail-analytics-platform/"
    "bronze/clickstream/"
)

OUTPUT_PATH = (
    "s3://retail-analytics-platform/"
    "gold/clickstream_sessions/"
)

spark = SparkSession.builder.appName(
    "ClickstreamSessionAnalytics"
).getOrCreate()

df = spark.read.json(
    "s3://retail-analytics-platform/bronze/clickstream/"
)

df = df.withColumn(
    "event_timestamp",
    to_timestamp("event_timestamp")
)

session_df = (
    df.groupBy(
        "session_id",
        "customer_id"
    )
    .agg(
        count("*").alias("total_events"),

        min("event_timestamp").alias(
            "session_start"
        ),

        max("event_timestamp").alias(
            "session_end"
        ),

        max(
            when(
                col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias("purchased")
    )
)

session_df = session_df.withColumn(
    "session_duration_seconds",

    unix_timestamp("session_end")
    -
    unix_timestamp("session_start")
)

output_path = (
    "s3://retail-analytics-platform/"
    "gold/clickstream_sessions/"
)

session_df.write.mode(
    "overwrite"
).parquet(
    output_path
)

spark.stop()
