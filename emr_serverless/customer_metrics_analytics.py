from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    sum,
    count,
    avg
)


spark = SparkSession.builder.appName(
    "CustomerMetricsAnalytics"
).getOrCreate()


# Read Silver Orders
df = spark.read.parquet(
    "s3://retail-analytics-platform/silver/orders/"
)


# Customer Aggregations
customer_metrics_df = (
    df.groupBy("customer_id")
    .agg(

        count("order_id").alias(
            "total_orders"
        ),

        sum("total_amount").alias(
            "total_spent"
        ),

        avg("total_amount").alias(
            "avg_order_value"
        ),

        sum("quantity").alias(
            "total_quantity_purchased"
        )
    )
)


# Write Gold Layer
customer_metrics_df.write.mode(
    "overwrite"
).parquet(
    "s3://retail-analytics-platform/gold/customer_metrics/"
)


spark.stop()
