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

print("Reading clickstream data...")

df = spark.read.json(
    INPUT_PATH
)

df = df.withColumn(
    "event_timestamp",
    to_timestamp("event_timestamp")
)

print("Calculating session analytics...")

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

print(
    f"Total sessions: {session_df.count()}"
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

print("Writing gold dataset to S3...")

session_df.write.mode(
    "overwrite"
).parquet(
    OUTPUT_PATH
)

print("Job completed successfully.")

spark.stop()
